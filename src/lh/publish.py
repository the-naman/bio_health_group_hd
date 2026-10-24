"""Publish the repo's dashboards (bi/*.lvdash.json) to one shared folder per environment (same file in both repos).

The files in the repo point at the dev catalogs. For each environment:
  1. the catalog names are swapped (bhg_dev. -> bhg_<env>., fqf_dev. -> fqf_<env>.); a dev name left in a
     test/prod copy stops the run
  2. the dashboard <name>_<env> is created or updated in <folder>/<env>; folder "~/..." = the home folder of
     whoever publishes (the job account in test/prod), because everything under /Workspace/Shared inherits
     Can Manage for all workspace users
  3. it is published WITHOUT embedded credentials: every viewer queries as themselves, so column masks
     and row filters apply to the person looking at it
  4. permissions are set exactly: managers CAN_MANAGE, viewer groups CAN_RUN (open and refresh)
Settings in config/bi.yml. plan mode changes nothing. Uses only the Databricks REST API of the caller."""

import glob
import json
import os
import re
import yaml

# the underscore may be markdown-escaped in text widgets: bhg\\_dev in the file (bhg\_dev on screen)
DEV = re.compile(r"\b(bhg|fqf)((?:\\\\)*)_dev\.")
LEFT = re.compile(r"\b(bhg|fqf)(?:\\\\)*_dev\b")


def load_bi(root):
    with open(os.path.join(root, "config", "bi.yml")) as f:
        return yaml.safe_load(f)


def for_env(text, env):
    """The dashboard text pointed at this environment's catalogs."""
    out = DEV.sub(lambda m: f"{m.group(1)}{m.group(2)}_{env}.", text)
    if env != "dev" and LEFT.search(out):
        raise ValueError(f"a dev catalog name is left in the {env} copy")
    return out


def dashboards(root, env):
    """[(name, serialized dashboard)] for every bi/*.lvdash.json in the repo, ready for env."""
    out = []
    for path in sorted(glob.glob(os.path.join(root, "bi", "*.lvdash.json"))):
        name = os.path.basename(path)[: -len(".lvdash.json")]
        with open(path) as f:
            text = for_env(f.read(), env)
        json.loads(text)  # must still be valid JSON
        out.append((name, text))
    return out


def acl(bi, env):
    return [{"group_name": g, "permission_level": "CAN_MANAGE"} for g in bi["managers"]] + [
        {"group_name": g, "permission_level": "CAN_RUN"} for g in bi["viewers"][env]
    ]


class Api:
    """Thin wrapper over the workspace REST API of whoever runs the notebook (or the job account)."""

    def __init__(self, w):
        self.w = w

    def call(self, method, path, body=None, query=None):
        return self.w.api_client.do(method, path, body=body, query=query) or {}

    def warehouse_id(self, name):
        for wh in self.call("GET", "/api/2.0/sql/warehouses").get("warehouses", []):
            if wh["name"] == name:
                return wh["id"]
        raise ValueError(f"warehouse '{name}' not found")

    def find(self, path):
        """Dashboard id at this workspace path, or None."""
        try:
            st = self.call("GET", "/api/2.0/workspace/get-status", query={"path": path})
        except Exception as e:  # not there yet -> None; any other error stops the run
            if type(e).__name__ in ("ResourceDoesNotExist", "NotFound") or "RESOURCE_DOES_NOT_EXIST" in str(e):
                return None
            raise
        return st.get("resource_id")

    def home(self):
        me = self.call("GET", "/api/2.0/preview/scim/v2/Me")
        return f"/Workspace/Users/{me['userName']}"

    def mkdirs(self, path):
        self.call("POST", "/api/2.0/workspace/mkdirs", body={"path": path})

    def create(self, name, parent, text, wh):
        body = {"display_name": name, "parent_path": parent, "serialized_dashboard": text, "warehouse_id": wh}
        return self.call("POST", "/api/2.0/lakeview/dashboards", body=body)["dashboard_id"]

    def update(self, did, name, text, wh):
        body = {"display_name": name, "serialized_dashboard": text, "warehouse_id": wh}
        self.call("PATCH", f"/api/2.0/lakeview/dashboards/{did}", body=body)

    def publish(self, did, wh):
        self.call(
            "POST",
            f"/api/2.0/lakeview/dashboards/{did}/published",
            body={"embed_credentials": False, "warehouse_id": wh},
        )

    def set_acl(self, did, entries):
        self.call("PUT", f"/api/2.0/permissions/dashboards/{did}", body={"access_control_list": entries})

    def published(self, did):
        return self.call("GET", f"/api/2.0/lakeview/dashboards/{did}/published")

    def get_acl(self, did):
        return self.call("GET", f"/api/2.0/permissions/dashboards/{did}")


def run(api, bi, root, env, apply=False):
    """Create or update, publish and share every dashboard of this repo for env. Returns one row each."""
    folder = bi["folder"]
    if folder.startswith("/Workspace/Shared"):
        raise ValueError("do not publish under /Workspace/Shared: every workspace user inherits Can Manage there")
    if folder.startswith("~/"):
        folder = api.home() + folder[1:]
    parent = f"{folder}/{env}"
    wh = api.warehouse_id(bi["warehouse"])
    entries = acl(bi, env)
    rows = []
    for name, text in dashboards(root, env):
        title = f"{name}_{env}"
        did = api.find(f"{parent}/{title}.lvdash.json")
        action = "update" if did else "create"
        if apply:
            api.mkdirs(parent)
            if did:
                api.update(did, title, text, wh)
            else:
                did = api.create(title, parent, text, wh)
            api.publish(did, wh)
            api.set_acl(did, entries)
        rows.append(
            {
                "dashboard": title,
                "action": action,
                "id": did,
                "path": f"{bi['folder']}/{env}",
                "viewers": ", ".join(e["group_name"] for e in entries if e["permission_level"] == "CAN_RUN"),
            }
        )
    return rows
