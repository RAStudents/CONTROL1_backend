CREATE TABLE tenant_roles (
    company_id uuid NOT NULL REFERENCES companies ON DELETE CASCADE,
    name text NOT NULL,
    permissions text[] NOT NULL,
    PRIMARY KEY(company_id, name)
);

CREATE FUNCTION initialize_tenant_roles(tenant uuid) RETURNS void LANGUAGE sql AS $$
    INSERT INTO tenant_roles(company_id,name,permissions) VALUES
        (tenant,'OPERATOR',ARRAY['EVENTS_READ','EVENTS_REVIEW','DEVICES_READ','DICTIONARIES_READ']),
        (tenant,'ADMIN',ARRAY['EVENTS_READ','EVENTS_REVIEW','DEVICES_READ','DEVICES_CREATE','DEVICES_UPDATE','DEVICES_DELETE',
            'REPORTS_READ','REPORTS_EXPORT','DICTIONARIES_READ','DICTIONARIES_MANAGE','AUDIT_READ','USERS_READ','USERS_MANAGE']),
        (tenant,'OBSERVER',ARRAY['EVENTS_READ','DEVICES_READ','DICTIONARIES_READ']),
        (tenant,'AUDITOR',ARRAY['EVENTS_READ','DEVICES_READ','DICTIONARIES_READ','AUDIT_READ','REPORTS_READ'])
    ON CONFLICT (company_id,name) DO NOTHING
$$;

SELECT initialize_tenant_roles(id) FROM companies;
CREATE FUNCTION initialize_company_roles() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM initialize_tenant_roles(NEW.id);
    RETURN NEW;
END $$;
CREATE TRIGGER initialize_company_roles AFTER INSERT ON companies
    FOR EACH ROW EXECUTE FUNCTION initialize_company_roles();
CREATE TRIGGER access_tenant_roles AFTER INSERT OR UPDATE OR DELETE ON tenant_roles
    FOR EACH STATEMENT EXECUTE FUNCTION notify_access_change();

ALTER TABLE memberships ADD COLUMN role text;
ALTER TABLE memberships ADD CONSTRAINT fk_membership_tenant_role
    FOREIGN KEY (company_id,role) REFERENCES tenant_roles(company_id,name);

CREATE INDEX idx_event_versions_gc ON event_versions (company_id, valid_to_sequence)
    WHERE valid_to_sequence IS NOT NULL;
CREATE INDEX idx_devices_company_location ON devices (company_id, location_id);
