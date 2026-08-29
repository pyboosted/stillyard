fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write("schema/stillyard-spec-v1.json", stillyard::schema_json()?)?;
    std::fs::write(
        "schema/stillyard-config-v1.json",
        stillyard::config_schema_json()?,
    )?;
    Ok(())
}
