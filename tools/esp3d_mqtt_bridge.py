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
    --topic-root printer/my-printer
"""

from __future__ import annotations

import argparse
import logging
import re
import signal
import socket
import sys
import json
import telnetlib
import time
from dataclasses import dataclass
from typing import Dict, Optional

import paho.mqtt.client as mqtt


M114_REGEX = re.compile(r"\b([XYZE])\s*:\s*(-?\d+(?:\.\d+)?)", re.IGNORECASE)
M105_TEMP_REGEX = re.compile(
    r"\b([TB])\s*:\s*(-?\d+(?:\.\d+)?)\s*/\s*(-?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

DEFAULT_ESP3D_HOST = "192.168.0.50"
DEFAULT_MQTT_HOST = "192.168.0.10"
DEFAULT_COMMAND_RESPONSE_TIMEOUT_S = 2.0
DEFAULT_RESPONSE_IDLE_GAP_S = 0.25


@dataclass
class BridgeConfig:
    esp3d_host: str
    telnet_port: int
    telnet_timeout_s: float
    mqtt_host: str
    mqtt_port: int
    mqtt_username: Optional[str]
    mqtt_password: Optional[str]
    topic_root: str
    topic_format: str
    poll_interval_s: float
    mqtt_qos: int
    mqtt_retain: bool
    command_response_timeout_s: float
    response_idle_gap_s: float


class Esp3dMqttBridge:
    def __init__(self, cfg: BridgeConfig) -> None:
        self.cfg = cfg
        self._running = True
        self._tn: Optional[telnetlib.Telnet] = None

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

    def connect(self) -> None:
        self._mqtt.connect(self.cfg.mqtt_host, self.cfg.mqtt_port, keepalive=60)
        self._mqtt.loop_start()
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

    def _topic_for(self, metric: str) -> str:
        return self.cfg.topic_format.format(root=self.cfg.topic_root.rstrip("/"), metric=metric)

    def _publish(self, metric: str, value: float) -> None:
        topic = self._topic_for(metric)
        payload = f"{value:.3f}".rstrip("0").rstrip(".")
        info = self._mqtt.publish(topic, payload=payload, qos=self.cfg.mqtt_qos, retain=self.cfg.mqtt_retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.error("MQTT publish failed topic=%s rc=%s", topic, info.rc)
        else:
            logging.info("Published MQTT topic=%s payload=%s", topic, payload)


    def _publish_bulk(self, metrics: Dict[str, float]) -> None:
        if not metrics:
            return
        topic = self.cfg.topic_root.rstrip("/")
        payload = json.dumps(metrics, separators=(",", ":"), sort_keys=True)
        info = self._mqtt.publish(topic, payload=payload, qos=self.cfg.mqtt_qos, retain=self.cfg.mqtt_retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.error("MQTT bulk publish failed topic=%s rc=%s", topic, info.rc)
        else:
            logging.info("Published MQTT bulk topic=%s payload=%s", topic, payload)

    def run(self) -> None:
        logging.info("Bridge started: polling every %.2fs", self.cfg.poll_interval_s)
        while self._running:
            try:
                m114_text = self._telnet_cmd_and_collect("M114")
                m105_text = self._telnet_cmd_and_collect("M105")
                pos = self._parse_m114(m114_text)
                temps = self._parse_m105(m105_text)

                metrics = {**pos, **temps}
                for metric, value in metrics.items():
                    self._publish(metric, value)
                self._publish_bulk(metrics)

                if not pos:
                    logging.warning("No XYZ values parsed from M114 response: %r", m114_text)
                if not temps:
                    logging.warning("No temperature values parsed from M105 response: %r", m105_text)

            except (socket.error, ConnectionError, EOFError) as err:
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

    parser.add_argument("--topic-root", default="printer/esp3d", help="Topic root/prefix")
    parser.add_argument(
        "--topic-format",
        default="{root}/{metric}",
        help="Topic template using placeholders {root} and {metric}",
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
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    return BridgeConfig(
        esp3d_host=args.esp3d_host,
        telnet_port=args.telnet_port,
        telnet_timeout_s=args.telnet_timeout_s,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_username=args.mqtt_username,
        mqtt_password=args.mqtt_password,
        topic_root=args.topic_root,
        topic_format=args.topic_format,
        poll_interval_s=args.poll_interval_s,
        mqtt_qos=args.mqtt_qos,
        mqtt_retain=args.mqtt_retain,
        command_response_timeout_s=args.command_response_timeout_s,
        response_idle_gap_s=args.response_idle_gap_s,
    )


def main() -> int:
    cfg = parse_args()
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
