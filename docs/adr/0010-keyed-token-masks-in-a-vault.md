# 0010 Keyed token masks, with the key in a vault catalog

- Status: Accepted (Oct 2026)
- Context: in test and prod, ids (patient, customer, loan, payment ...) are shown to readers as tokens
  `PREFIX#XXXXXXXX` so joins and counts still work. The first token was a plain SHA-256 of the id. Ids follow simple
  patterns (`PAT000123`), so anyone who can read tokens could hash every possible id and turn tokens back into ids.
  Roles hold catalog-level SELECT and EXECUTE in test/prod, so a key kept anywhere in `<code>_<env>` would be readable.
- Decision: a separate catalog `lh_vault`, one schema per catalog (`lh_vault.bhg_test` ...). Only the job account
  `sp_<code>_<env>` has access, and only to its own schema (`setup_once/notebook/04_vault_setup`, rules in
  `src/lh/access.py`). On its first run it creates `mask_key` (64 random hex characters from the operating system's
  secure random source, written as data so it never appears in a query history) and the token function
  `mask_token`, which hashes key + id. `privacy.apply` moves every token column to that function and drops the
  plain one. Dev keeps the plain token (synthetic data, every dev role sees real values anyway).
- Proof before building (24 Oct 2026, dev, throwaway objects): a Developer with no vault access read a masked column
  and got the keyed token, identical to the owner's result; reading the key table and calling the function directly
  both failed with INSUFFICIENT_PERMISSIONS. So a mask runs with its owner's rights, and readers cannot hash guessed
  ids themselves.
- Consequences: tokens in test/prod change once, on the first run after the release (anything that stored a token
  value, such as an alert already mailed, may appear once more). The key is never rotated on purpose: a new key
  changes every token. If the vault is lost, the next run creates a new key and the tokens change; no data is lost.
  Management owns the vault catalog (it created it) but cannot read the key either: the key table belongs to the job
  account, and catalog ownership does not give SELECT on it (proven 24 Oct by 02_verify_setup).
  The hash is SHA-256 over key + value (Databricks SQL has no HMAC function); with a secret 256-bit key and 8 shown
  characters this is enough to stop guessing.
