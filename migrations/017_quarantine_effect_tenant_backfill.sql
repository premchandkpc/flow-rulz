-- Repair effect quarantine records created before tenant identity was carried
-- through the outbox. Do not infer ownership from an HTTP request.
UPDATE quarantine AS quarantined
SET tenant_id = effects.tenant_id
FROM outbox_effects AS effects
WHERE quarantined.source_type = 'effect'
  AND quarantined.source_id = effects.effect_id
  AND (quarantined.tenant_id IS NULL OR quarantined.tenant_id = '')
  AND effects.tenant_id <> '';
