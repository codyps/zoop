#[tokio_macros::main]
async fn main() {
    let pools = zfs_cmd_api::pools().await.unwrap();
    for pool in pools {
        println!("Pool: {}", pool);
    }
}
