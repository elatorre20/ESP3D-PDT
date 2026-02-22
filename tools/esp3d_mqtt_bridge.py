#!/usr/bin/env python3
"""
ESP3D -> MQTT bridge.

Polls an ESP3D HTTP endpoint for printer status using forwarded G-code commands
(M114 for XYZ position and M105 for temperatures), parses results, and publishes
the values to MQTT topics.

Dependencies:
  pip install requests paho-mqtt

Example:
  python3 tools/esp3d_mqtt_bridge.py \
    --esp3d-base-url http://192.168.1.50 \
    --mqtt-host 192.168.1.10 \
    --topic-root printer/my-printer
"""

from __future__ import annotations

import argparse
import logging
import re
import signal
import sys
import time
from dataclasses import dataclass
from typing import Dict, Optional
from urllib.parse import quote

import requests
import paho.mqtt.client as mqtt


M114_REGEX = re.compile(r"\b([XYZ])\s*:\s*(-?\d+(?:\.\d+)?)", re.IGNORECASE)
M105_TEMP_REGEX = re.compile(
    r"\b([TB])\s*:\s*(-?\d+(?:\.\d+)?)\s*/\s*(-?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


@dataclass
class BridgeConfig:
    esp3d_base_url: str
    mqtt_host: str
    mqtt_port: int
    mqtt_username: Optional[str]
    mqtt_password: Optional[str]
    topic_root: str
    topic_format: str
    poll_interval_s: float
    request_timeout_s: float
    mqtt_qos: int
    mqtt_retain: bool


class Esp3dMqttBridge:
    def __init__(self, cfg: BridgeConfig) -> None:
        self.cfg = cfg
        self._running = True
        self._http = requests.Session()

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

    def close(self) -> None:
        self._mqtt.loop_stop()
        self._mqtt.disconnect()
        self._http.close()

    def _esp3d_command(self, cmd: str) -> str:
        encoded = quote(cmd, safe="")
        url = f"{self.cfg.esp3d_base_url.rstrip('/')}/command?cmd={encoded}"
        logging.debug("GET %s", url)
        response = self._http.get(url, timeout=self.cfg.request_timeout_s)
        response.raise_for_status()
        text = response.text.strip()
        logging.debug("ESP3D response for %s: %r", cmd, text)
        return text

    @staticmethod
    def _parse_m114(text: str) -> Dict[str, float]:
        values: Dict[str, float] = {}
        for axis, value in M114_REGEX.findall(text):
            axis = axis.lower()
            if axis in ("x", "y", "z"):
                values[axis] = float(value)
        return values

    @staticmethod
    def _parse_m105(text: str) -> Dict[str, float]:
        values: Dict[str, float] = {}
        # Handles common forms like: "ok T:205.3 /210.0 B:60.0 /60.0 ..."
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
        # Default format: "{root}/{metric}".
        return self.cfg.topic_format.format(root=self.cfg.topic_root.rstrip("/"), metric=metric)

    def _publish(self, metric: str, value: float) -> None:
        topic = self._topic_for(metric)
        payload = f"{value:.3f}".rstrip("0").rstrip(".")
        info = self._mqtt.publish(topic, payload=payload, qos=self.cfg.mqtt_qos, retain=self.cfg.mqtt_retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            logging.error("MQTT publish failed topic=%s rc=%s", topic, info.rc)
        else:
            logging.debug("Published %s=%s", topic, payload)

    def run(self) -> None:
        logging.info("Bridge started: polling every %.2fs", self.cfg.poll_interval_s)
        while self._running:
            try:
                m114_text = self._esp3d_command("M114")
                pos = self._parse_m114(m114_text)

                m105_text = self._esp3d_command("M105")
                temps = self._parse_m105(m105_text)

                for metric, value in {**pos, **temps}.items():
                    self._publish(metric, value)

                if not pos:
                    logging.warning("No XYZ values parsed from M114 response: %r", m114_text)
                if not temps:
                    logging.warning("No temperature values parsed from M105 response: %r", m105_text)

            except requests.RequestException as err:
                logging.error("ESP3D request failed: %s", err)
            except Exception as err:  # Keep service alive on parse/runtime issues.
                logging.exception("Bridge loop error: %s", err)

            time.sleep(self.cfg.poll_interval_s)


def parse_args() -> BridgeConfig:
    parser = argparse.ArgumentParser(description="Poll ESP3D printer status and publish to MQTT")
    parser.add_argument("--esp3d-base-url", required=True, help="ESP3D URL, e.g. http://192.168.1.50")

    parser.add_argument("--mqtt-host", required=True, help="MQTT broker hostname/IP")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    parser.add_argument("--mqtt-username", default=None, help="MQTT username")
    parser.add_argument("--mqtt-password", default=None, help="MQTT password")

    parser.add_argument(
        "--topic-root",
        default="printer/esp3d",
        help="Topic root/prefix, used by topic format template",
    )
    parser.add_argument(
        "--topic-format",
        default="{root}/{metric}",
        help="Topic format template with placeholders {root} and {metric}",
    )

    parser.add_argument("--poll-interval-s", type=float, default=2.0, help="Polling interval in seconds")
    parser.add_argument("--request-timeout-s", type=float, default=4.0, help="HTTP request timeout in seconds")

    parser.add_argument("--mqtt-qos", type=int, choices=[0, 1, 2], default=0, help="MQTT QoS")
    parser.add_argument("--mqtt-retain", action="store_true", help="Set MQTT retain flag")

    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    return BridgeConfig(
        esp3d_base_url=args.esp3d_base_url,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_username=args.mqtt_username,
        mqtt_password=args.mqtt_password,
        topic_root=args.topic_root,
        topic_format=args.topic_format,
        poll_interval_s=args.poll_interval_s,
        request_timeout_s=args.request_timeout_s,
        mqtt_qos=args.mqtt_qos,
        mqtt_retain=args.mqtt_retain,
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
