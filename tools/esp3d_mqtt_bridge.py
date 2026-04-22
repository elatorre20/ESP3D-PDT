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
DEFAULT_SENSOR_TYPE_CATEGORY_ID = 1000
DEFAULT_COMMAND_RESPONSE_TIMEOUT_S = 2.0
DEFAULT_RESPONSE_IDLE_GAP_S = 0.25
DEFAULT_SIM_SPEED_MM_S = 30.0
DEFAULT_SIM_DIAMETER_MM = 100.0
DEFAULT_SIM_LAYER_HEIGHT_MM = 5.0
DEFAULT_SIM_LAYERS = 10

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
    sensor_type_category_id: int
    name: str
    location_id: str
    poll_interval_s: float
    mqtt_qos: int
    mqtt_retain: bool
    command_response_timeout_s: float
    response_idle_gap_s: float
    simulate: bool
    simulation_speed_mm_s: float
    simulation_diameter_mm: float
    simulation_layer_height_mm: float
    simulation_layers: int


class Esp3dMqttBridge:
    def __init__(self, cfg: BridgeConfig) -> None:
        self.cfg = cfg
        self._running = True
        self._tn: Optional[telnetlib.Telnet] = None
        self._sim_elapsed_s = 0.0
        self._sim_angle_rad = 0.0
        self._sim_z_mm = 0.0
        self._sim_circle_count = 0

        self._mqtt = mqtt.Client()
        if cfg.mqtt_username:
            self._mqtt.username_pw_set(cfg.mqtt_username, cfg.mqtt_password)
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT broker %s:%d", self.cfg.mqtt_host, self.cfg.mqtt_port)
        else:
            logging.error("MQTT connect failed with rc=%s", rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            logging.warning("Unexpected MQTT disconnect rc=%s", rc)

    def stop(self) -> None:
        logging.info("Stopping bridge...")
        self._running = False

    def update_simulation_settings(
        self,
        *,
        speed_mm_s: Optional[float] = None,
        diameter_mm: Optional[float] = None,
        layer_height_mm: Optional[float] = None,
        layers: Optional[int] = None,
    ) -> None:
        if speed_mm_s is not None:
            self.cfg.simulation_speed_mm_s = speed_mm_s
        if diameter_mm is not None:
            self.cfg.simulation_diameter_mm = diameter_mm
        if layer_height_mm is not None:
            self.cfg.simulation_layer_height_mm = layer_height_mm
        if layers is not None:
            self.cfg.simulation_layers = layers

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

    def _telnet_cmd_and_collect(self, cmd: str) -> str:
        self._ensure_telnet()
        assert self._tn is not None

        self._drain_telnet(0.1)
        logging.info("Sending printer command: %s", cmd)
        self._tn.write((cmd + "\n").encode("utf-8"))

        end = time.time() + self.cfg.command_response_timeout_s
        last_data_time: Optional[float] = None
        chunks = []
        while time.time() < end:
            try:
                data = self._tn.read_very_eager()
            except EOFError as err:
                raise ConnectionError("ESP3D telnet connection closed") from err
            if data:
                now = time.time()
                last_data_time = now
                chunks.append(data.decode("utf-8", errors="replace"))
                continue
            else:
                # If we already received bytes for this command and line stays idle,
                # treat the response as complete.
                if last_data_time is not None and (time.time() - last_data_time) >= self.cfg.response_idle_gap_s:
                    break
                time.sleep(0.02)

        text = "".join(chunks)
        logging.info("Raw response for %s: %r", cmd, text)
        return text

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
        while self._running:
            try:
                if self.cfg.simulate:
                    metrics = self._generate_simulated_metrics()
                    for metric, value in metrics.items():
                        self._publish(metric, value)
                else:
                    m114_text = self._telnet_cmd_and_collect("M114")
                    m105_text = self._telnet_cmd_and_collect("M105")
                    pos = self._parse_m114(m114_text)
                    temps = self._parse_m105(m105_text)

                    metrics = {**pos, **temps}
                    for metric, value in metrics.items():
                        self._publish(metric, value)

                    if not pos:
                        logging.warning("No XYZ values parsed from M114 response: %r", m114_text)
                    if not temps:
                        logging.warning("No temperature values parsed from M105 response: %r", m105_text)

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

            time.sleep(self.cfg.poll_interval_s)

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

        extruder_actual = 220.0 + 2.5 * math.sin(self._sim_elapsed_s * 0.9) + 0.6 * math.sin(self._sim_elapsed_s * 2.3)
        bed_actual = 90.0 + 1.5 * math.sin(self._sim_elapsed_s * 0.5 + 0.8) + 0.4 * math.sin(self._sim_elapsed_s * 1.7)

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

    return BridgeConfig(
        esp3d_host=args.esp3d_host,
        telnet_port=args.telnet_port,
        telnet_timeout_s=args.telnet_timeout_s,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_username=args.mqtt_username,
        mqtt_password=args.mqtt_password,
        mqtt_topic=args.mqtt_topic,
        sensor_type_category_id=args.sensor_type_category_id,
        name=args.name,
        location_id=args.location_id,
        poll_interval_s=args.poll_interval_s,
        mqtt_qos=args.mqtt_qos,
        mqtt_retain=args.mqtt_retain,
        command_response_timeout_s=args.command_response_timeout_s,
        response_idle_gap_s=args.response_idle_gap_s,
        simulate=args.simulate,
        simulation_speed_mm_s=args.speed,
        simulation_diameter_mm=args.diameter,
        simulation_layer_height_mm=args.layer_height,
        simulation_layers=args.layers,
    ), args.gui


class BridgeGui:
    def __init__(self, cfg: BridgeConfig) -> None:
        import tkinter as tk
        from tkinter import messagebox, simpledialog

        self._tk = tk
        self._messagebox = messagebox
        self._simpledialog = simpledialog
        self._cfg = copy.deepcopy(cfg)
        self._bridge: Optional[Esp3dMqttBridge] = None
        self._worker: Optional[threading.Thread] = None

        self.root = tk.Tk()
        self.root.title("ESP3D MQTT Bridge Control")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.mode_var = tk.StringVar(value="Sim" if cfg.simulate else "Real")
        self.mqtt_ip_var = tk.StringVar(value=cfg.mqtt_host)
        self.esp_ip_var = tk.StringVar(value=cfg.esp3d_host)
        self.status_var = tk.StringVar(value="Disconnected")

        self._build_ui()

    def _build_ui(self) -> None:
        tk = self._tk
        tk.Label(self.root, text="Telemetry Mode:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        self.mode_button = tk.Button(self.root, textvariable=self.mode_var, command=self._toggle_mode, width=20)
        self.mode_button.grid(row=0, column=1, padx=8, pady=6)

        tk.Label(self.root, text="MQTT Broker IP:").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        tk.Entry(self.root, textvariable=self.mqtt_ip_var, width=24).grid(row=1, column=1, padx=8, pady=6)

        tk.Label(self.root, text="ESP IP:").grid(row=2, column=0, sticky="w", padx=8, pady=6)
        tk.Entry(self.root, textvariable=self.esp_ip_var, width=24).grid(row=2, column=1, padx=8, pady=6)

        tk.Button(self.root, text="Reconnect (New IPs)", command=self._reconnect_with_ips, width=20).grid(
            row=3, column=0, columnspan=2, padx=8, pady=8
        )

        tk.Label(self.root, text="Simulation Controls:").grid(row=4, column=0, sticky="w", padx=8, pady=8)
        self.diameter_btn = tk.Button(self.root, text="Set Circle Diameter", command=self._set_diameter, width=20)
        self.diameter_btn.grid(row=5, column=0, padx=8, pady=4)
        self.layer_height_btn = tk.Button(self.root, text="Set Layer Height", command=self._set_layer_height, width=20)
        self.layer_height_btn.grid(row=5, column=1, padx=8, pady=4)
        self.layers_btn = tk.Button(self.root, text="Set Layer Number", command=self._set_layers, width=20)
        self.layers_btn.grid(row=6, column=0, padx=8, pady=4)
        self.speed_btn = tk.Button(self.root, text="Set Speed", command=self._set_speed, width=20)
        self.speed_btn.grid(row=6, column=1, padx=8, pady=4)
        self.reset_btn = tk.Button(self.root, text="Reset to Bottom Layer Start", command=self._reset_pattern, width=42)
        self.reset_btn.grid(row=7, column=0, columnspan=2, padx=8, pady=8)

        tk.Label(self.root, textvariable=self.status_var, anchor="w", fg="blue").grid(
            row=8, column=0, columnspan=2, sticky="w", padx=8, pady=8
        )

        self._update_sim_buttons_state()

    def _update_sim_buttons_state(self) -> None:
        state = self._tk.NORMAL if self._cfg.simulate else self._tk.DISABLED
        for btn in (self.diameter_btn, self.layer_height_btn, self.layers_btn, self.speed_btn, self.reset_btn):
            btn.configure(state=state)

    def _start_bridge(self) -> None:
        self._bridge = Esp3dMqttBridge(copy.deepcopy(self._cfg))
        self._bridge.connect()

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

    def _restart_bridge(self) -> None:
        self._stop_bridge()
        self._start_bridge()

    def _toggle_mode(self) -> None:
        self._cfg.simulate = not self._cfg.simulate
        self.mode_var.set("Sim" if self._cfg.simulate else "Real")
        self._update_sim_buttons_state()
        self._restart_bridge()

    def _reconnect_with_ips(self) -> None:
        mqtt_ip = self.mqtt_ip_var.get().strip()
        esp_ip = self.esp_ip_var.get().strip()
        if not mqtt_ip or not esp_ip:
            self._messagebox.showerror("Invalid Input", "MQTT broker IP and ESP IP are both required.")
            return
        self._cfg.mqtt_host = mqtt_ip
        self._cfg.esp3d_host = esp_ip
        self._restart_bridge()

    def _set_diameter(self) -> None:
        value = self._simpledialog.askfloat("Circle Diameter", "Diameter (mm):", minvalue=0.001)
        if value:
            self._cfg.simulation_diameter_mm = value
            if self._bridge:
                self._bridge.update_simulation_settings(diameter_mm=value)

    def _set_layer_height(self) -> None:
        value = self._simpledialog.askfloat("Layer Height", "Layer height (mm):", minvalue=0.001)
        if value:
            self._cfg.simulation_layer_height_mm = value
            if self._bridge:
                self._bridge.update_simulation_settings(layer_height_mm=value)

    def _set_layers(self) -> None:
        value = self._simpledialog.askinteger("Layer Number", "Layer count:", minvalue=1)
        if value:
            self._cfg.simulation_layers = value
            if self._bridge:
                self._bridge.update_simulation_settings(layers=value)

    def _set_speed(self) -> None:
        value = self._simpledialog.askfloat("Speed", "Speed (mm/s):", minvalue=0.001)
        if value:
            self._cfg.simulation_speed_mm_s = value
            if self._bridge:
                self._bridge.update_simulation_settings(speed_mm_s=value)

    def _reset_pattern(self) -> None:
        if self._bridge:
            self._bridge.reset_simulation_pattern()

    def _on_close(self) -> None:
        self._stop_bridge()
        self.root.destroy()

    def run(self) -> None:
        self._start_bridge()
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
