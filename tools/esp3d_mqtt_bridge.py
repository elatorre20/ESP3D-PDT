#!/usr/bin/env python3
"""
ESP3D -> MQTT bridge.

This bridge uses a **full-duplex Telnet connection** to ESP3D so it can read real
printer responses (e.g. M114 / M105 output), then publishes parsed values to MQTT.

Why Telnet mode:
- ESP3D HTTP `/command?cmd=...` returns "ESP3D says: command forwarded" for
  non-ESP commands, which does not contain printer telemetry.
- Telnet/WebSocket are full-duplex and receive printer answers directly.

Dependencies:
  pip install paho-mqtt

Example (uses default ESP3D/MQTT hosts):
  python3 tools/esp3d_mqtt_bridge.py

Example (override hosts):
  python3 tools/esp3d_mqtt_bridge.py \
    --esp3d-host 192.168.1.50 \
    --mqtt-host 192.168.1.10 \
    --name printer \
    --location-id printer001
"""

from __future__ import annotations

import argparse
import copy
import logging
import math
import random
import re
import signal
import socket
import sys
import json
import telnetlib
import threading
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Dict, Optional

import paho.mqtt.client as mqtt


M114_REGEX = re.compile(r"\b([XYZE])\s*:\s*(-?\d+(?:\.\d+)?)", re.IGNORECASE)
M105_TEMP_REGEX = re.compile(
    r"\b([TB])\s*:\s*(-?\d+(?:\.\d+)?)\s*/\s*(-?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

DEFAULT_ESP3D_HOST = "192.168.1.254"
DEFAULT_MQTT_HOST = "192.168.1.105"
DEFAULT_MQTT_TOPIC = "PDT/Printer/SensorMsg"
DEFAULT_MQTT_COMMAND_TOPIC = "PDT/EdgeDevice/ActuatorCmd"
DEFAULT_SENSOR_TYPE_CATEGORY_ID = 1000
DEFAULT_COMMAND_RESPONSE_TIMEOUT_S = 2.0
DEFAULT_RESPONSE_IDLE_GAP_S = 0.25
DEFAULT_M114_RETRIES = 2
DEFAULT_BUSY_RETRY_DELAY_S = 0.15
DEFAULT_SIM_SPEED_MM_S = 10.0
DEFAULT_SIM_DIAMETER_MM = 100.0
DEFAULT_SIM_LAYER_HEIGHT_MM = 2
DEFAULT_SIM_LAYERS = 10
DEFAULT_SIM_EXTRUDER_TARGET_C = 240.0
DEFAULT_SIM_BED_TARGET_C = 90.0
DEFAULT_SIM_TEMP_VARIANCE_C = 2.0

METRIC_TYPE_MAPPING = {
    "x": ("x_pos", 1),
    "y": ("y_pos", 2),
    "z": ("z_pos", 3),
    "extruder_actual": ("hotend_temp", 4),
    "bed_actual": ("bed_temp", 5),
}


@dataclass
class BridgeConfig:
    esp3d_host: str
    telnet_port: int
    telnet_timeout_s: float
    mqtt_host: str
    mqtt_port: int
    mqtt_username: Optional[str]
    mqtt_password: Optional[str]
    mqtt_topic: str
    mqtt_command_topic: str
    sensor_type_category_id: int
    name: str
    location_id: str
    poll_interval_s: float
    mqtt_qos: int
    mqtt_retain: bool
    command_response_timeout_s: float
    response_idle_gap_s: float
    m114_retries: int
    busy_retry_delay_s: float
    simulate: bool
    simulation_speed_mm_s: float
    simulation_diameter_mm: float
    simulation_layer_height_mm: float
    simulation_layers: int
    simulation_extruder_target_c: float
    simulation_bed_target_c: float
    simulation_realistic_temp_inertia: bool
    simulation_temp_variance_c: float


class Esp3dMqttBridge:
    def __init__(self, cfg: BridgeConfig) -> None:
        self.cfg = cfg
        self._running = True
        self._tn: Optional[telnetlib.Telnet] = None
        self._telnet_lock = threading.RLock()
        self._sim_elapsed_s = 0.0
        self._sim_angle_rad = 0.0
        self._sim_z_mm = 0.0
        self._sim_circle_count = 0
        self._sim_extruder_target_setpoint = cfg.simulation_extruder_target_c
        self._sim_bed_target_setpoint = cfg.simulation_bed_target_c
        self._sim_extruder_target_current = cfg.simulation_extruder_target_c
        self._sim_bed_target_current = cfg.simulation_bed_target_c
        self._sim_extruder_actual = max(20.0, cfg.simulation_extruder_target_c - 12.0)
        self._sim_bed_actual = max(20.0, cfg.simulation_bed_target_c - 6.0)
        self._sim_realistic_temp_inertia = cfg.simulation_realistic_temp_inertia
        self._sim_temp_variance_c = cfg.simulation_temp_variance_c
        self._sim_extruder_noise_current = 0.0
        self._sim_bed_noise_current = 0.0
        self._sim_extruder_noise_target = 0.0
        self._sim_bed_noise_target = 0.0
        self._next_realtime_poll_cmd = "M105"

        self._mqtt = mqtt.Client()
        if cfg.mqtt_username:
            self._mqtt.username_pw_set(cfg.mqtt_username, cfg.mqtt_password)
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_disconnect = self._on_disconnect
        self._mqtt.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT broker %s:%d", self.cfg.mqtt_host, self.cfg.mqtt_port)
            for topic in self._build_command_topic_subscriptions(self.cfg.mqtt_command_topic):
                client.subscribe(topic, qos=self.cfg.mqtt_qos)
                logging.info("Subscribed to command topic: %s", topic)
        else:
            logging.error("MQTT connect failed with rc=%s", rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logging.warning("Unexpected MQTT disconnect rc=%s", rc)

    def _on_message(self, client, userdata, msg) -> None:
        _ = client, userdata
        if self.cfg.simulate:
            logging.info("Ignoring incoming command in simulation mode topic=%s", msg.topic)
            return

        payload = msg.payload.decode("utf-8", errors="replace")
        is_command_topic = self._is_command_topic(msg.topic)
        looks_like_command = self._payload_looks_like_command(payload)
        if not is_command_topic and not looks_like_command:
            logging.debug("Ignoring non-command MQTT message topic=%s", msg.topic)
            return

        logging.info("Received command candidate topic=%s payload=%s", msg.topic, payload)

        cmd, parse_error = self._extract_printer_command(payload)
        if not cmd:
            if is_command_topic:
                logging.error(
                    "Invalid command message on topic=%s: %s payload=%s",
                    msg.topic,
                    parse_error or "unknown parsing issue",
                    payload,
                )
            else:
                logging.debug("Ignoring command-like payload that could not be parsed from topic=%s", msg.topic)
            return

        logging.info("Forwarding command from MQTT topic=%s -> %s", msg.topic, cmd)
        try:
            self._telnet_send_only(cmd)
        except Exception:
            logging.exception("Failed forwarding command to ESP3D printer: %s", cmd)

    @staticmethod
    def _build_command_topic_subscriptions(base_topic: str) -> list[str]:
        base_topic = base_topic.strip()
        topics = {base_topic}

        if "/ActuatorCmd" in base_topic:
            topics.add(base_topic.replace("/ActuatorCmd", "/actuator_cmd"))
        if "/actuator_cmd" in base_topic:
            topics.add(base_topic.replace("/actuator_cmd", "/ActuatorCmd"))
        if "/EdgeDevice/" in base_topic:
            topics.add(base_topic.replace("/EdgeDevice/", "/edge_device/"))
        if "/edge_device/" in base_topic:
            topics.add(base_topic.replace("/edge_device/", "/EdgeDevice/"))

        return sorted(t for t in topics if t)

    @staticmethod
    def _is_command_topic(topic: str) -> bool:
        topic_l = (topic or "").lower()
        return "actuator" in topic_l or topic_l.endswith("/cmd") or "command" in topic_l

    @staticmethod
    def _payload_looks_like_command(payload: str) -> bool:
        payload_l = (payload or "").lower()
        return ("\"commandname\"" in payload_l or "\"propertyname\"" in payload_l) and "\"value\"" in payload_l

    def stop(self) -> None:
        logging.info("Stopping bridge...")
        self._running = False

    def _sleep_with_stop(self, duration_s: float) -> None:
        end = time.time() + max(0.0, duration_s)
        while self._running and time.time() < end:
            time.sleep(min(0.05, end - time.time()))

    def update_simulation_settings(
        self,
        *,
        speed_mm_s: Optional[float] = None,
        diameter_mm: Optional[float] = None,
        layer_height_mm: Optional[float] = None,
        layers: Optional[int] = None,
        extruder_target_c: Optional[float] = None,
        bed_target_c: Optional[float] = None,
        realistic_temp_inertia: Optional[bool] = None,
        temp_variance_c: Optional[float] = None,
    ) -> None:
        if speed_mm_s is not None:
            self.cfg.simulation_speed_mm_s = speed_mm_s
        if diameter_mm is not None:
            self.cfg.simulation_diameter_mm = diameter_mm
        if layer_height_mm is not None:
            self.cfg.simulation_layer_height_mm = layer_height_mm
        if layers is not None:
            self.cfg.simulation_layers = layers
        if extruder_target_c is not None:
            self.cfg.simulation_extruder_target_c = extruder_target_c
            self._sim_extruder_target_setpoint = extruder_target_c
        if bed_target_c is not None:
            self.cfg.simulation_bed_target_c = bed_target_c
            self._sim_bed_target_setpoint = bed_target_c
        if realistic_temp_inertia is not None:
            self.cfg.simulation_realistic_temp_inertia = realistic_temp_inertia
            self._sim_realistic_temp_inertia = realistic_temp_inertia
        if temp_variance_c is not None:
            self.cfg.simulation_temp_variance_c = temp_variance_c
            self._sim_temp_variance_c = temp_variance_c
            self._sim_extruder_noise_current = max(-temp_variance_c, min(temp_variance_c, self._sim_extruder_noise_current))
            self._sim_bed_noise_current = max(-temp_variance_c, min(temp_variance_c, self._sim_bed_noise_current))

    def reset_simulation_pattern(self) -> None:
        self._sim_elapsed_s = 0.0
        self._sim_angle_rad = 0.0
        self._sim_z_mm = 0.0
        self._sim_circle_count = 0
        logging.info("Simulation pattern reset to bottom layer start")

    def connect(self) -> None:
        self._mqtt.connect(self.cfg.mqtt_host, self.cfg.mqtt_port, keepalive=60)
        self._mqtt.loop_start()
        logging.info("Using sensor typeCategoryID=%d", self.cfg.sensor_type_category_id)
        if self.cfg.simulate:
            logging.info("Simulation mode enabled: publishing generated printer telemetry.")
        else:
            self._connect_telnet()

    def close(self) -> None:
        self._mqtt.loop_stop()
        self._mqtt.disconnect()
        if self._tn:
            self._tn.close()
            self._tn = None

    def _connect_telnet(self) -> None:
        logging.info("Connecting to ESP3D telnet %s:%d", self.cfg.esp3d_host, self.cfg.telnet_port)
        with self._telnet_lock:
            self._tn = telnetlib.Telnet(self.cfg.esp3d_host, self.cfg.telnet_port, self.cfg.telnet_timeout_s)
            self._drain_telnet(0.2)

    def _ensure_telnet(self) -> None:
        if self._tn is None:
            self._connect_telnet()

    def _drain_telnet(self, duration_s: float) -> None:
        end = time.time() + duration_s
        while self._tn and time.time() < end:
            try:
                chunk = self._tn.read_very_eager()
            except EOFError:
                break
            if not chunk:
                time.sleep(0.02)
            else:
                logging.debug("Drained telnet bytes: %r", chunk.decode("utf-8", errors="replace"))

    def _read_pending_telnet(self, duration_s: float = 0.05) -> str:
        if not self._tn:
            return ""
        end = time.time() + max(0.0, duration_s)
        chunks = []
        while self._tn and time.time() < end:
            try:
                chunk = self._tn.read_very_eager()
            except EOFError:
                break
            if not chunk:
                break
            chunks.append(chunk.decode("utf-8", errors="replace"))
        return "".join(chunks)

    @staticmethod
    def _response_has_busy_message(text: str) -> bool:
        text_l = text.lower()
        return "busy: processing" in text_l or "echo:busy" in text_l

    def _telnet_cmd_and_collect(self, cmd: str, *, expect_pattern: Optional[re.Pattern] = None) -> str:
        with self._telnet_lock:
            self._ensure_telnet()
            assert self._tn is not None

            pending_text = self._read_pending_telnet(0.05)
            if pending_text:
                logging.debug("Pre-command pending telnet bytes kept for parsing: %r", pending_text)
            logging.info("Sending printer command: %s", cmd)
            self._tn.write((cmd + "\r\n").encode("utf-8"))

            end = time.time() + self.cfg.command_response_timeout_s
            last_data_time: Optional[float] = None
            got_expected = bool(expect_pattern and pending_text and expect_pattern.search(pending_text))
            chunks = [pending_text] if pending_text else []
            while time.time() < end:
                try:
                    data = self._tn.read_very_eager()
                except EOFError as err:
                    raise ConnectionError("ESP3D telnet connection closed") from err
                if data:
                    now = time.time()
                    last_data_time = now
                    chunks.append(data.decode("utf-8", errors="replace"))
                    if expect_pattern and expect_pattern.search("".join(chunks)):
                        got_expected = True
                    continue
                else:
                    # If we already received bytes for this command and line stays idle:
                    # - with expected data pattern, stop only after it has been observed
                    # - otherwise use generic idle-gap completion.
                    if (
                        last_data_time is not None
                        and (time.time() - last_data_time) >= self.cfg.response_idle_gap_s
                        and (got_expected or expect_pattern is None)
                    ):
                        break
                    time.sleep(0.02)

        text = "".join(chunks)
        logging.info("Raw response for %s: %r", cmd, text)
        return text

    def _poll_with_retries(
        self,
        cmd: str,
        parse_func,
        *,
        expect_pattern: Optional[re.Pattern] = None,
        retries: int = 0,
    ) -> tuple[str, Dict[str, float]]:
        last_text = ""
        for attempt in range(retries + 1):
            last_text = self._telnet_cmd_and_collect(cmd, expect_pattern=expect_pattern)
            parsed = parse_func(last_text)
            if parsed:
                return last_text, parsed

            if attempt >= retries:
                break

            if self._response_has_busy_message(last_text):
                delay = self.cfg.busy_retry_delay_s * (attempt + 1)
                logging.info(
                    "%s received busy response without telemetry; retrying in %.2fs (attempt %d/%d)",
                    cmd,
                    delay,
                    attempt + 1,
                    retries,
                )
                time.sleep(delay)
            else:
                break

        return last_text, {}

    def _telnet_send_only(self, cmd: str) -> None:
        with self._telnet_lock:
            self._ensure_telnet()
            assert self._tn is not None
            logging.info("Raw printer command: %s", cmd)
            self._tn.write((cmd + "\r\n").encode("utf-8"))

    @staticmethod
    def _extract_printer_command(payload: str) -> tuple[Optional[str], Optional[str]]:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return None, "payload is not valid JSON"

        command_name_exact = None
        command_name_fallback = None
        command_value = None
        stack = [data]

        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                for key, value in cur.items():
                    key_l = str(key).lower()
                    if command_name_exact is None and key_l in ("commandname", "propertyname"):
                        if isinstance(value, str):
                            command_name_exact = value.strip()
                    if command_name_fallback is None and key_l == "name":
                        if isinstance(value, str):
                            command_name_fallback = value.strip()
                    if command_value is None and key_l in ("value", "targetvalue"):
                        if isinstance(value, (int, float, str)):
                            command_value = value
                    if isinstance(value, (dict, list)):
                        stack.append(value)
            elif isinstance(cur, list):
                stack.extend(cur)

        command_name = command_name_exact or command_name_fallback
        if not command_name:
            return None, "missing command name (expected commandName/propertyName)"

        def _to_num(value) -> Optional[float]:
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return None
                try:
                    return float(value)
                except ValueError:
                    return None
            return None

        value_num = _to_num(command_value)
        name_key = command_name.strip().lower()

        if name_key in ("printspeedpercent", "printspeedpercentage", "printspeedpct"):
            if value_num is None:
                return None, "print speed command missing numeric value"
            return f"M220 S{int(round(value_num))}", None
        if name_key in ("targethotendtempc", "hotendtargettemperature", "hotendtargettempc"):
            if value_num is None:
                return None, "hotend target temperature command missing numeric value"
            return f"M104 S{round(value_num, 1):g}", None
        if name_key in ("targetbedtempc", "bedtargettemperature", "bedtargettempc"):
            if value_num is None:
                return None, "bed target temperature command missing numeric value"
            return f"M140 S{round(value_num, 1):g}", None

        return None, f"unsupported command name '{command_name}'"

    @staticmethod
    def _parse_m114(text: str) -> Dict[str, float]:
        values: Dict[str, float] = {}
        # Marlin M114 can include machine step counters after "Count"
        # (e.g. "Count X:13200 Y:12240 Z:8340"). Keep the first XYZ values,
        # which are the Cartesian position values we want to publish.
        for axis, value in M114_REGEX.findall(text):
            axis = axis.lower()
            if axis in ("x", "y", "z") and axis not in values:
                values[axis] = float(value)
            elif axis == "e" and "extruder_position" not in values:
                values["extruder_position"] = float(value)
        return values

    @staticmethod
    def _parse_m105(text: str) -> Dict[str, float]:
        values: Dict[str, float] = {}
        for sensor, actual, target in M105_TEMP_REGEX.findall(text):
            sensor = sensor.upper()
            if sensor == "T":
                values["extruder_actual"] = float(actual)
                values["extruder_target"] = float(target)
            elif sensor == "B":
                values["bed_actual"] = float(actual)
                values["bed_target"] = float(target)
        return values

    def _parse_all_telemetry(self, text: str) -> Dict[str, float]:
        telemetry: Dict[str, float] = {}
        telemetry.update(self._parse_m114(text))
        telemetry.update(self._parse_m105(text))
        return telemetry

    def _publish(self, metric: str, value: float) -> None:
        if metric not in METRIC_TYPE_MAPPING:
            logging.debug("Skipping unsupported metric for MQTT payload format: %s", metric)
            return

        type_name, type_id = METRIC_TYPE_MAPPING[metric]
        payload = json.dumps(
            {
                "timeOffsetSeconds": 0.0,
                "timeStamp": datetime.now(timezone.utc).isoformat(),
                "hasError": False,
                # Unity SensorData handlers match telemetry channels by `name`
                # (e.g., x_pos, y_pos, z_pos, hotend_temp, bed_temp).
                "name": type_name,
                "typeID": type_id,
                "statusCode": 0,
                "latitude": 0.0,
                "longitude": 0.0,
                "elevation": 0.0,
                "locationID": self.cfg.location_id,
                "typeName": type_name,
                "typeCategoryID": self.cfg.sensor_type_category_id,
                "deviceID": self.cfg.location_id,
                "dataContainerType": "LabBenchStudios.Pdt.Data.SensorData",
                "isEnabled": True,
                "isSystemOnline": True,
                "value": value,
            },
            separators=(",", ":")
        )
        info = self._mqtt.publish(self.cfg.mqtt_topic, payload=payload, qos=self.cfg.mqtt_qos, retain=self.cfg.mqtt_retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.error("MQTT publish failed topic=%s rc=%s", self.cfg.mqtt_topic, info.rc)
        else:
            logging.info("Published MQTT topic=%s payload=%s", self.cfg.mqtt_topic, payload)

    def run(self) -> None:
        logging.info("Bridge started: polling every %.2fs", self.cfg.poll_interval_s)
        half_period_s = max(0.0, self.cfg.poll_interval_s / 2.0)
        if not self.cfg.simulate:
            logging.info("Real mode staggered polling: alternating M105/M114 every %.2fs", half_period_s)
        while self._running:
            try:
                if self.cfg.simulate:
                    metrics = self._generate_simulated_metrics()
                    for metric, value in metrics.items():
                        self._publish(metric, value)
                else:
                    if self._next_realtime_poll_cmd == "M105":
                        m105_text, temps = self._poll_with_retries(
                            "M105",
                            self._parse_m105,
                            expect_pattern=M105_TEMP_REGEX,
                            retries=0,
                        )
                        telemetry = self._parse_all_telemetry(m105_text)
                        for metric, value in telemetry.items():
                            self._publish(metric, value)
                        if not temps:
                            logging.warning("No temperature values parsed from M105 response: %r", m105_text)
                        if not telemetry:
                            logging.warning("No telemetry values parsed from M105 response: %r", m105_text)
                        self._next_realtime_poll_cmd = "M114"
                    else:
                        m114_text, pos = self._poll_with_retries(
                            "M114",
                            self._parse_m114,
                            expect_pattern=M114_REGEX,
                            retries=self.cfg.m114_retries,
                        )
                        telemetry = self._parse_all_telemetry(m114_text)
                        for metric, value in telemetry.items():
                            self._publish(metric, value)
                        if not pos:
                            logging.warning("No XYZ values parsed from M114 response: %r", m114_text)
                        if not telemetry:
                            logging.warning("No telemetry values parsed from M114 response: %r", m114_text)
                        self._next_realtime_poll_cmd = "M105"

            except (socket.error, ConnectionError, EOFError) as err:
                if self.cfg.simulate:
                    logging.exception("Simulation loop error: %s", err)
                    time.sleep(self.cfg.poll_interval_s)
                    continue
                logging.warning("Telnet connection issue: %s; reconnecting", err)
                if self._tn:
                    try:
                        self._tn.close()
                    except Exception:
                        pass
                    self._tn = None
                time.sleep(1.0)
                try:
                    self._connect_telnet()
                except Exception as conn_err:
                    logging.error("Telnet reconnect failed: %s", conn_err)
            except Exception as err:
                logging.exception("Bridge loop error: %s", err)

            self._sleep_with_stop(self.cfg.poll_interval_s if self.cfg.simulate else half_period_s)

    def _generate_simulated_metrics(self) -> Dict[str, float]:
        radius_mm = self.cfg.simulation_diameter_mm / 2.0
        speed_mm_s = self.cfg.simulation_speed_mm_s
        angular_speed_rad_s = speed_mm_s / radius_mm
        layer_height_mm = self.cfg.simulation_layer_height_mm
        max_height_mm = layer_height_mm * self.cfg.simulation_layers

        dt = self.cfg.poll_interval_s
        self._sim_elapsed_s += dt
        self._sim_angle_rad += angular_speed_rad_s * dt

        current_circles = int(self._sim_angle_rad / (2.0 * math.pi))
        if current_circles > self._sim_circle_count:
            self._sim_circle_count = current_circles
            if self._sim_z_mm >= max_height_mm:
                self._sim_z_mm = 0.0
                self._sim_circle_count = 0
                self._sim_angle_rad = math.fmod(self._sim_angle_rad, 2.0 * math.pi)
            else:
                self._sim_z_mm = min(max_height_mm, self._sim_z_mm + layer_height_mm)

        phase = math.fmod(self._sim_angle_rad, 2.0 * math.pi)
        build_plate_center_x_mm = 150.0
        build_plate_center_y_mm = 150.0
        x = build_plate_center_x_mm + (radius_mm * math.cos(phase))
        y = build_plate_center_y_mm + (radius_mm * math.sin(phase))

        if self._sim_realistic_temp_inertia:
            max_target_step = dt * 1.0
            self._sim_extruder_target_current += max(
                -max_target_step,
                min(max_target_step, self._sim_extruder_target_setpoint - self._sim_extruder_target_current),
            )
            self._sim_bed_target_current += max(
                -max_target_step,
                min(max_target_step, self._sim_bed_target_setpoint - self._sim_bed_target_current),
            )
        else:
            self._sim_extruder_target_current = self._sim_extruder_target_setpoint
            self._sim_bed_target_current = self._sim_bed_target_setpoint

        extruder_delta = self._sim_extruder_target_current - self._sim_extruder_actual
        extruder_rate = 3.0 if extruder_delta > 0 else 1.8
        self._sim_extruder_actual += max(-extruder_rate * dt, min(extruder_rate * dt, extruder_delta))

        bed_delta = self._sim_bed_target_current - self._sim_bed_actual
        bed_rate = 1.2 if bed_delta > 0 else 0.8
        self._sim_bed_actual += max(-bed_rate * dt, min(bed_rate * dt, bed_delta))

        variance_range = max(0.0, self._sim_temp_variance_c)
        if variance_range == 0.0:
            self._sim_extruder_noise_current = 0.0
            self._sim_bed_noise_current = 0.0
            self._sim_extruder_noise_target = 0.0
            self._sim_bed_noise_target = 0.0
        else:
            # Keep random jitter, but with strong inertia so fluctuations do not jump too fast.
            self._sim_extruder_noise_target = random.uniform(-variance_range, variance_range)
            self._sim_bed_noise_target = random.uniform(-variance_range, variance_range)
            max_noise_step = dt * 0.12  # C/s
            self._sim_extruder_noise_current += max(
                -max_noise_step,
                min(max_noise_step, self._sim_extruder_noise_target - self._sim_extruder_noise_current),
            )
            self._sim_bed_noise_current += max(
                -max_noise_step,
                min(max_noise_step, self._sim_bed_noise_target - self._sim_bed_noise_current),
            )

        extruder_actual = self._sim_extruder_actual + self._sim_extruder_noise_current
        bed_actual = self._sim_bed_actual + self._sim_bed_noise_current

        return {
            "x": x,
            "y": y,
            "z": self._sim_z_mm,
            "extruder_actual": extruder_actual,
            "bed_actual": bed_actual,
        }


def parse_args() -> BridgeConfig:
    parser = argparse.ArgumentParser(description="Poll ESP3D printer status and publish to MQTT")

    parser.add_argument(
        "--esp3d-host",
        default=DEFAULT_ESP3D_HOST,
        help=f"ESP3D hostname/IP (default: {DEFAULT_ESP3D_HOST})",
    )
    parser.add_argument("--telnet-port", type=int, default=23, help="ESP3D Telnet port (default: 23)")
    parser.add_argument("--telnet-timeout-s", type=float, default=5.0, help="Telnet connect timeout")

    parser.add_argument(
        "--mqtt-host",
        default=DEFAULT_MQTT_HOST,
        help=f"MQTT broker hostname/IP (default: {DEFAULT_MQTT_HOST})",
    )
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    parser.add_argument("--mqtt-username", default=None, help="MQTT username")
    parser.add_argument("--mqtt-password", default=None, help="MQTT password")

    parser.add_argument(
        "--mqtt-topic",
        default=DEFAULT_MQTT_TOPIC,
        help=f"MQTT publish topic (default: {DEFAULT_MQTT_TOPIC})",
    )
    parser.add_argument(
        "--mqtt-command-topic",
        default=DEFAULT_MQTT_COMMAND_TOPIC,
        help=f"MQTT subscribe topic for incoming actuator commands (default: {DEFAULT_MQTT_COMMAND_TOPIC})",
    )
    parser.add_argument(
        "--sensor-type-category-id",
        type=int,
        default=DEFAULT_SENSOR_TYPE_CATEGORY_ID,
        help=f"Sensor typeCategoryID payload value (default: {DEFAULT_SENSOR_TYPE_CATEGORY_ID})",
    )
    parser.add_argument("--name", default="printer", help="Message name field")
    parser.add_argument(
        "--location-id",
        default="printer001",
        help="Message locationID/deviceID field",
    )

    parser.add_argument("--poll-interval-s", type=float, default=2.0, help="Polling interval in seconds")
    parser.add_argument(
        "--command-response-timeout-s",
        type=float,
        default=DEFAULT_COMMAND_RESPONSE_TIMEOUT_S,
        help=f"Max seconds to wait for response bytes (default: {DEFAULT_COMMAND_RESPONSE_TIMEOUT_S})",
    )
    parser.add_argument(
        "--response-idle-gap-s",
        type=float,
        default=DEFAULT_RESPONSE_IDLE_GAP_S,
        help=f"Consider response complete after this idle gap (default: {DEFAULT_RESPONSE_IDLE_GAP_S})",
    )
    parser.add_argument(
        "--m114-retries",
        type=int,
        default=DEFAULT_M114_RETRIES,
        help=f"Number of M114 retries when only busy/no position is received (default: {DEFAULT_M114_RETRIES})",
    )
    parser.add_argument(
        "--busy-retry-delay-s",
        type=float,
        default=DEFAULT_BUSY_RETRY_DELAY_S,
        help=f"Base delay before retrying after busy response (default: {DEFAULT_BUSY_RETRY_DELAY_S})",
    )
    parser.add_argument("--mqtt-qos", type=int, choices=[0, 1, 2], default=0, help="MQTT QoS")
    parser.add_argument("--mqtt-retain", action="store_true", help="Set MQTT retain flag")
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Publish simulated printer movement/temperatures instead of polling ESP3D",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=DEFAULT_SIM_SPEED_MM_S,
        help=f"Simulation mode movement speed in mm/s (default: {DEFAULT_SIM_SPEED_MM_S})",
    )
    parser.add_argument(
        "--diameter",
        type=float,
        default=DEFAULT_SIM_DIAMETER_MM,
        help=f"Simulation mode circular movement diameter in mm (default: {DEFAULT_SIM_DIAMETER_MM})",
    )
    parser.add_argument(
        "--layer_height",
        type=float,
        default=DEFAULT_SIM_LAYER_HEIGHT_MM,
        help=f"Simulation mode layer height in mm (default: {DEFAULT_SIM_LAYER_HEIGHT_MM})",
    )
    parser.add_argument(
        "--layers",
        type=int,
        default=DEFAULT_SIM_LAYERS,
        help=f"Simulation mode layer count before resetting Z to 0 (default: {DEFAULT_SIM_LAYERS})",
    )
    parser.add_argument(
        "--sim-extruder-target-c",
        type=float,
        default=DEFAULT_SIM_EXTRUDER_TARGET_C,
        help=f"Simulation mode extruder target temperature in C (default: {DEFAULT_SIM_EXTRUDER_TARGET_C})",
    )
    parser.add_argument(
        "--sim-bed-target-c",
        type=float,
        default=DEFAULT_SIM_BED_TARGET_C,
        help=f"Simulation mode bed target temperature in C (default: {DEFAULT_SIM_BED_TARGET_C})",
    )
    parser.add_argument(
        "--sim-realistic-temp-inertia",
        action="store_true",
        help="Simulation mode target temperature inertia (slew-limited to 1 C/s)",
    )
    parser.add_argument(
        "--sim-temp-variance-c",
        type=float,
        default=DEFAULT_SIM_TEMP_VARIANCE_C,
        help=f"Simulation mode temperature noise range in C (default: {DEFAULT_SIM_TEMP_VARIANCE_C})",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--gui", action="store_true", help="Launch interactive GUI controls")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.speed <= 0:
        parser.error("--speed must be greater than 0")
    if args.diameter <= 0:
        parser.error("--diameter must be greater than 0")
    if args.layer_height <= 0:
        parser.error("--layer_height must be greater than 0")
    if args.layers <= 0:
        parser.error("--layers must be greater than 0")
    if args.sim_extruder_target_c < 0:
        parser.error("--sim-extruder-target-c must be >= 0")
    if args.sim_bed_target_c < 0:
        parser.error("--sim-bed-target-c must be >= 0")
    if args.sim_temp_variance_c < 0:
        parser.error("--sim-temp-variance-c must be >= 0")
    if args.m114_retries < 0:
        parser.error("--m114-retries must be >= 0")
    if args.busy_retry_delay_s < 0:
        parser.error("--busy-retry-delay-s must be >= 0")

    return BridgeConfig(
        esp3d_host=args.esp3d_host,
        telnet_port=args.telnet_port,
        telnet_timeout_s=args.telnet_timeout_s,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_username=args.mqtt_username,
        mqtt_password=args.mqtt_password,
        mqtt_topic=args.mqtt_topic,
        mqtt_command_topic=args.mqtt_command_topic,
        sensor_type_category_id=args.sensor_type_category_id,
        name=args.name,
        location_id=args.location_id,
        poll_interval_s=args.poll_interval_s,
        mqtt_qos=args.mqtt_qos,
        mqtt_retain=args.mqtt_retain,
        command_response_timeout_s=args.command_response_timeout_s,
        response_idle_gap_s=args.response_idle_gap_s,
        m114_retries=args.m114_retries,
        busy_retry_delay_s=args.busy_retry_delay_s,
        simulate=args.simulate,
        simulation_speed_mm_s=args.speed,
        simulation_diameter_mm=args.diameter,
        simulation_layer_height_mm=args.layer_height,
        simulation_layers=args.layers,
        simulation_extruder_target_c=args.sim_extruder_target_c,
        simulation_bed_target_c=args.sim_bed_target_c,
        simulation_realistic_temp_inertia=args.sim_realistic_temp_inertia,
        simulation_temp_variance_c=args.sim_temp_variance_c,
    ), args.gui


class BridgeGui:
    def __init__(self, cfg: BridgeConfig) -> None:
        import tkinter as tk
        from tkinter import messagebox

        self._tk = tk
        self._messagebox = messagebox
        self._cfg = copy.deepcopy(cfg)
        self._bridge: Optional[Esp3dMqttBridge] = None
        self._worker: Optional[threading.Thread] = None

        self.root = tk.Tk()
        self.root.title("ESP3D MQTT Bridge Control")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.mode_var = tk.StringVar(value="Sim" if cfg.simulate else "Real")
        self.mqtt_ip_var = tk.StringVar(value=cfg.mqtt_host)
        self.esp_ip_var = tk.StringVar(value=cfg.esp3d_host)
        self.data_rate_var = tk.StringVar(value=str(cfg.poll_interval_s))
        self.sim_diameter_var = tk.StringVar(value=str(cfg.simulation_diameter_mm))
        self.sim_layer_height_var = tk.StringVar(value=str(cfg.simulation_layer_height_mm))
        self.sim_layers_var = tk.StringVar(value=str(cfg.simulation_layers))
        self.sim_speed_var = tk.StringVar(value=str(cfg.simulation_speed_mm_s))
        self.sim_extruder_target_var = tk.StringVar(value=str(cfg.simulation_extruder_target_c))
        self.sim_bed_target_var = tk.StringVar(value=str(cfg.simulation_bed_target_c))
        self.sim_realistic_temp_inertia_var = tk.BooleanVar(value=cfg.simulation_realistic_temp_inertia)
        self.sim_temp_variance_var = tk.StringVar(value=str(cfg.simulation_temp_variance_c))
        self.status_var = tk.StringVar(value="Disconnected")

        self._build_ui()

    def _build_ui(self) -> None:
        tk = self._tk
        tk.Label(self.root, text="Telemetry Mode:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        mode_frame = tk.Frame(self.root)
        mode_frame.grid(row=0, column=1, padx=8, pady=6, sticky="w")
        self.real_mode_btn = tk.Button(mode_frame, text="Real", command=self._set_real_mode, width=9)
        self.real_mode_btn.grid(row=0, column=0, padx=(0, 4))
        self.sim_mode_btn = tk.Button(mode_frame, text="Sim", command=self._set_sim_mode, width=9)
        self.sim_mode_btn.grid(row=0, column=1)
        self._refresh_mode_button_styles()

        tk.Label(self.root, text="MQTT Broker IP:").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        tk.Entry(self.root, textvariable=self.mqtt_ip_var, width=24).grid(row=1, column=1, padx=8, pady=6)

        tk.Label(self.root, text="ESP IP:").grid(row=2, column=0, sticky="w", padx=8, pady=6)
        tk.Entry(self.root, textvariable=self.esp_ip_var, width=24).grid(row=2, column=1, padx=8, pady=6)

        tk.Label(self.root, text="Data Rate (s):").grid(row=3, column=0, sticky="w", padx=8, pady=6)
        tk.Entry(self.root, textvariable=self.data_rate_var, width=24).grid(row=3, column=1, padx=8, pady=6)
        tk.Button(self.root, text="Apply Data Rate", command=self._apply_data_rate, width=20).grid(
            row=4, column=0, columnspan=2, padx=8, pady=4
        )

        tk.Button(self.root, text="Reconnect (New IPs)", command=self._reconnect_with_ips, width=20).grid(
            row=5, column=0, columnspan=2, padx=8, pady=8
        )
        self.connect_btn = tk.Button(self.root, text="Connect", command=self._start_bridge, width=20)
        self.connect_btn.grid(row=6, column=0, padx=8, pady=(0, 8))
        self.disconnect_btn = tk.Button(self.root, text="Disconnect", command=self._stop_bridge, width=20)
        self.disconnect_btn.grid(row=6, column=1, padx=8, pady=(0, 8))

        tk.Label(self.root, text="Simulation Controls:").grid(row=7, column=0, sticky="w", padx=8, pady=8)
        tk.Label(self.root, text="Circle Diameter (mm):").grid(row=8, column=0, sticky="w", padx=8, pady=4)
        self.sim_diameter_entry = tk.Entry(self.root, textvariable=self.sim_diameter_var, width=24)
        self.sim_diameter_entry.grid(row=8, column=1, padx=8, pady=4)
        tk.Label(self.root, text="Layer Height (mm):").grid(row=9, column=0, sticky="w", padx=8, pady=4)
        self.sim_layer_height_entry = tk.Entry(self.root, textvariable=self.sim_layer_height_var, width=24)
        self.sim_layer_height_entry.grid(row=9, column=1, padx=8, pady=4)
        tk.Label(self.root, text="Layer Number:").grid(row=10, column=0, sticky="w", padx=8, pady=4)
        self.sim_layers_entry = tk.Entry(self.root, textvariable=self.sim_layers_var, width=24)
        self.sim_layers_entry.grid(row=10, column=1, padx=8, pady=4)
        tk.Label(self.root, text="Speed (mm/s):").grid(row=11, column=0, sticky="w", padx=8, pady=4)
        self.sim_speed_entry = tk.Entry(self.root, textvariable=self.sim_speed_var, width=24)
        self.sim_speed_entry.grid(row=11, column=1, padx=8, pady=4)
        tk.Label(self.root, text="Extruder Target (C):").grid(row=12, column=0, sticky="w", padx=8, pady=4)
        self.sim_extruder_target_entry = tk.Entry(self.root, textvariable=self.sim_extruder_target_var, width=24)
        self.sim_extruder_target_entry.grid(row=12, column=1, padx=8, pady=4)
        tk.Label(self.root, text="Bed Target (C):").grid(row=13, column=0, sticky="w", padx=8, pady=4)
        self.sim_bed_target_entry = tk.Entry(self.root, textvariable=self.sim_bed_target_var, width=24)
        self.sim_bed_target_entry.grid(row=13, column=1, padx=8, pady=4)
        self.sim_realistic_temp_inertia_check = tk.Checkbutton(
            self.root,
            text="Realistic temperature inertia (1 C/s slew)",
            variable=self.sim_realistic_temp_inertia_var,
        )
        self.sim_realistic_temp_inertia_check.grid(row=14, column=0, columnspan=2, sticky="w", padx=8, pady=4)
        tk.Label(self.root, text="Temp Variance Noise ±(C):").grid(row=15, column=0, sticky="w", padx=8, pady=4)
        self.sim_temp_variance_entry = tk.Entry(self.root, textvariable=self.sim_temp_variance_var, width=24)
        self.sim_temp_variance_entry.grid(row=15, column=1, padx=8, pady=4)

        self.update_sim_btn = tk.Button(self.root, text="Update All Sim Settings", command=self._update_all_sim_settings, width=42)
        self.update_sim_btn.grid(row=16, column=0, columnspan=2, padx=8, pady=8)
        self.reset_btn = tk.Button(self.root, text="Reset to Bottom Layer Start", command=self._reset_pattern, width=42)
        self.reset_btn.grid(row=17, column=0, columnspan=2, padx=8, pady=6)

        tk.Label(self.root, textvariable=self.status_var, anchor="w", fg="blue").grid(
            row=18, column=0, columnspan=2, sticky="w", padx=8, pady=8
        )

        self._update_sim_buttons_state()

    def _update_sim_buttons_state(self) -> None:
        state = self._tk.NORMAL if self._cfg.simulate else self._tk.DISABLED
        for widget in (
            self.sim_diameter_entry,
            self.sim_layer_height_entry,
            self.sim_layers_entry,
            self.sim_speed_entry,
            self.sim_extruder_target_entry,
            self.sim_bed_target_entry,
            self.sim_realistic_temp_inertia_check,
            self.sim_temp_variance_entry,
            self.update_sim_btn,
            self.reset_btn,
        ):
            widget.configure(state=state)

    def _start_bridge(self) -> None:
        if self._bridge is not None:
            self.status_var.set(f"Connected ({'Sim' if self._cfg.simulate else 'Real'} mode)")
            return

        self._bridge = Esp3dMqttBridge(copy.deepcopy(self._cfg))
        try:
            self._bridge.connect()
        except Exception as err:
            self._bridge = None
            self._worker = None
            self.status_var.set(f"Connection failed: {err}")
            self._messagebox.showerror("Connection Error", f"Unable to connect:\n{err}")
            return

        def _run_bridge() -> None:
            assert self._bridge is not None
            try:
                self._bridge.run()
            finally:
                self._bridge.close()

        self._worker = threading.Thread(target=_run_bridge, daemon=True)
        self._worker.start()
        self.status_var.set(f"Connected ({'Sim' if self._cfg.simulate else 'Real'} mode)")

    def _stop_bridge(self) -> None:
        if self._bridge is not None:
            self._bridge.stop()
        if self._worker is not None:
            self._worker.join(timeout=3.0)
        self._bridge = None
        self._worker = None
        self.status_var.set("Disconnected")

    def _restart_bridge(self, force_connect: bool = False) -> None:
        was_connected = self._bridge is not None
        self._stop_bridge()
        if was_connected or force_connect:
            self._start_bridge()

    def _refresh_mode_button_styles(self) -> None:
        if self._cfg.simulate:
            self.sim_mode_btn.configure(relief=self._tk.SUNKEN, bg="lightgreen")
            self.real_mode_btn.configure(relief=self._tk.RAISED, bg="SystemButtonFace")
        else:
            self.real_mode_btn.configure(relief=self._tk.SUNKEN, bg="lightgreen")
            self.sim_mode_btn.configure(relief=self._tk.RAISED, bg="SystemButtonFace")

    def _set_mode(self, simulate: bool) -> None:
        if self._cfg.simulate == simulate:
            return
        self._cfg.simulate = simulate
        self.mode_var.set("Sim" if self._cfg.simulate else "Real")
        self._refresh_mode_button_styles()
        self._update_sim_buttons_state()
        self._restart_bridge()

    def _set_real_mode(self) -> None:
        self._set_mode(False)

    def _set_sim_mode(self) -> None:
        self._set_mode(True)

    def _apply_data_rate(self) -> None:
        try:
            poll_interval_s = float(self.data_rate_var.get().strip())
        except ValueError:
            self._messagebox.showerror("Invalid Input", "Data rate must be a number in seconds.")
            return
        if poll_interval_s <= 0:
            self._messagebox.showerror("Invalid Input", "Data rate must be greater than 0.")
            return
        self._cfg.poll_interval_s = poll_interval_s
        self._restart_bridge()

    def _reconnect_with_ips(self) -> None:
        mqtt_ip = self.mqtt_ip_var.get().strip()
        esp_ip = self.esp_ip_var.get().strip()
        if not mqtt_ip or not esp_ip:
            self._messagebox.showerror("Invalid Input", "MQTT broker IP and ESP IP are both required.")
            return
        self._cfg.mqtt_host = mqtt_ip
        self._cfg.esp3d_host = esp_ip
        self._restart_bridge(force_connect=True)

    def _update_all_sim_settings(self) -> None:
        try:
            diameter = float(self.sim_diameter_var.get().strip())
            layer_height = float(self.sim_layer_height_var.get().strip())
            layers = int(self.sim_layers_var.get().strip())
            speed = float(self.sim_speed_var.get().strip())
            extruder_target = float(self.sim_extruder_target_var.get().strip())
            bed_target = float(self.sim_bed_target_var.get().strip())
            realistic_temp_inertia = bool(self.sim_realistic_temp_inertia_var.get())
            temp_variance = float(self.sim_temp_variance_var.get().strip())
        except ValueError:
            self._messagebox.showerror(
                "Invalid Input",
                "Simulation settings must be valid numeric values.",
            )
            return

        if diameter <= 0 or layer_height <= 0 or layers <= 0 or speed <= 0:
            self._messagebox.showerror(
                "Invalid Input",
                "Diameter, layer height, layer number, and speed must all be greater than 0.",
            )
            return
        if extruder_target < 0 or bed_target < 0:
            self._messagebox.showerror(
                "Invalid Input",
                "Extruder and bed target temperatures must be greater than or equal to 0.",
            )
            return
        if temp_variance < 0:
            self._messagebox.showerror(
                "Invalid Input",
                "Temperature variance noise range must be greater than or equal to 0.",
            )
            return

        self._cfg.simulation_diameter_mm = diameter
        self._cfg.simulation_layer_height_mm = layer_height
        self._cfg.simulation_layers = layers
        self._cfg.simulation_speed_mm_s = speed
        self._cfg.simulation_extruder_target_c = extruder_target
        self._cfg.simulation_bed_target_c = bed_target
        self._cfg.simulation_realistic_temp_inertia = realistic_temp_inertia
        self._cfg.simulation_temp_variance_c = temp_variance

        if self._bridge:
            self._bridge.update_simulation_settings(
                diameter_mm=diameter,
                layer_height_mm=layer_height,
                layers=layers,
                speed_mm_s=speed,
                extruder_target_c=extruder_target,
                bed_target_c=bed_target,
                realistic_temp_inertia=realistic_temp_inertia,
                temp_variance_c=temp_variance,
            )

    def _reset_pattern(self) -> None:
        if self._bridge:
            self._bridge.reset_simulation_pattern()

    def _on_close(self) -> None:
        self._stop_bridge()
        self.root.destroy()

    def run(self) -> None:
        self.status_var.set("Disconnected (click Connect to start)")
        self.root.mainloop()


def main() -> int:
    cfg, gui_mode = parse_args()
    if gui_mode:
        app = BridgeGui(cfg)
        app.run()
        return 0

    bridge = Esp3dMqttBridge(cfg)

    def _handle_signal(signum, frame):
        _ = signum, frame
        bridge.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        bridge.connect()
        bridge.run()
    finally:
        bridge.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
