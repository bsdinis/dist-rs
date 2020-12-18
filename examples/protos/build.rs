use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    tonic_build::configure()
        //.type_attribute(".", "#[derive(Serialize)]")
        .compile(&["./src/state_machine_server.proto"], &["./src/"])?;

    Ok(())
}
