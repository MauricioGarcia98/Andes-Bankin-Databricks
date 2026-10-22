"""Salesforce falso para los tests: upsert por Andes_Key__c (bulk2) y SOQL mínimo (campos, FROM, SystemModstamp >=)."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

PREFIX = {"Account": "001", "Contact": "003", "Lead": "00Q", "Opportunity": "006", "Case": "500"}


class _Bulk2Type:
    def __init__(self, sf, sobject):
        self.sf, self.sobject = sf, sobject

    def upsert(self, records, external_id_field="Andes_Key__c"):
        self.sf.upsert(self.sobject, records)
        return [{"numberRecordsProcessed": len(records), "numberRecordsFailed": 0, "job_id": "fake"}]


class _Bulk2:
    def __init__(self, sf):
        self.sf = sf

    def __getattr__(self, sobject):
        return _Bulk2Type(self.sf, sobject)


class FakeSalesforce:
    def __init__(self):
        self.data: dict[str, dict[str, dict]] = {o: {} for o in PREFIX}   # objeto → Id → registro
        self.clock = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
        self.bulk2 = _Bulk2(self)

    def _tick(self) -> str:
        self.clock += timedelta(seconds=1)
        return self.clock.strftime("%Y-%m-%dT%H:%M:%S.000+0000")

    def _by_key(self, obj, key):
        return next((r for r in self.data[obj].values() if r.get("Andes_Key__c") == key), None)

    def upsert(self, obj, rows):
        for row in rows:
            row = dict(row)
            parent_key = row.pop("Account.Andes_Key__c", None)
            if parent_key:
                row["AccountId"] = self._by_key("Account", parent_key)["Id"]
            rec = self._by_key(obj, row["Andes_Key__c"])
            ts = self._tick()
            if rec is None:
                rid = f"{PREFIX[obj]}{len(self.data[obj]) + 1:015d}"
                rec = {"Id": rid, "IsDeleted": False, "CreatedDate": ts}
                if obj == "Case":
                    rec["CaseNumber"] = f"{len(self.data[obj]) + 1:08d}"
                self.data[obj][rid] = rec
            rec.update(row, SystemModstamp=ts)
            if obj == "Opportunity":
                rec["IsClosed"] = rec["StageName"].startswith("Closed")
                rec["IsWon"] = rec["StageName"] == "Closed Won"
                rec["Amount"] = float(rec["Amount"]) if rec.get("Amount") else None
            if obj == "Case":
                rec["IsClosed"] = rec["Status"] == "Closed"
                rec["ClosedDate"] = ts if rec["IsClosed"] else None
            if obj == "Lead":
                rec["IsConverted"] = False

    def delete(self, obj, key):
        rec = self._by_key(obj, key)
        rec.update(IsDeleted=True, SystemModstamp=self._tick())

    def query_all(self, soql, include_deleted=False):
        m = re.match(r"SELECT (.+) FROM (\w+)(?: WHERE SystemModstamp >= (\S+))? ORDER BY SystemModstamp", soql)
        fields, obj, since = [f.strip() for f in m.group(1).split(",")], m.group(2), m.group(3)
        out = []
        for rec in self.data[obj].values():
            if rec["IsDeleted"] and not include_deleted:
                continue
            if since and rec["SystemModstamp"][:19] < since.rstrip("Z"):
                continue
            out.append({"attributes": {"type": obj}, **{f: rec.get(f) for f in fields}})
        return {"records": sorted(out, key=lambda r: r["SystemModstamp"])}
