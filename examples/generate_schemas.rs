fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write("schema/stillyard-spec-v4.json", stillyard::schema_json()?)?;
    std::fs::write(
        "schema/stillyard-config-v2.json",
        stillyard::config_schema_json()?,
    )?;
    std::fs::write(
        "schema/stillyard-managed-execution-v3.json",
        stillyard::managed_execution_schema_json()?,
    )?;
    Ok(())
}
