-- map_group_customer: one row per company member (BHG patient or FQF master customer) -> the group person
-- it belongs to. group_customer_key is a hash of the person's anchor, so it is stable across rebuilds.
SELECT company_code, member_id, xxhash64(anchor) AS group_customer_key
FROM _grp_matched
