import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  stages: [
    { duration: "30s", target: 20 },
    { duration: "1m", target: 50 },
    { duration: "30s", target: 0 },
  ],
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<1200"],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://127.0.0.1:8765";

export default function () {
  const health = http.get(`${BASE_URL}/api/health`);
  check(health, {
    "health returns 200": (res) => res.status === 200,
  });

  const focus = http.get(`${BASE_URL}/api/focus-snapshot`);
  check(focus, {
    "focus snapshot returns 200": (res) => res.status === 200,
    "focus snapshot is json": (res) => String(res.headers["Content-Type"] || "").includes("application/json"),
  });

  sleep(1);
}
