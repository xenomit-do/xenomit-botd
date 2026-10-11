use std::{
    thread,
    time::{Duration, Instant},
};
use xenomit_game_api::LiveFrame;
fn pct(values: &mut [f64], p: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() - 1) as f64 * p) as usize]
}
fn main() {
    let pid = std::env::args()
        .nth(1)
        .expect("Flash host PID")
        .parse()
        .expect("numeric PID");
    let client = xenomit_game_api::connect_flash_native(pid).unwrap();
    let mut frame = LiveFrame::default();
    let deadline = Instant::now() + Duration::from_secs(120);
    while !client.read_live_frame(&mut frame) {
        assert!(Instant::now() < deadline, "no fresh native frame");
        thread::sleep(Duration::from_millis(100));
    }
    let mut costs = Vec::new();
    let mut successes = 0;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(5) {
        let at = Instant::now();
        let status = client.status().unwrap();
        costs.push(at.elapsed().as_secs_f64() * 1e6);
        successes += usize::from(status.ready);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(client.read_live_frame(&mut frame));
    println!(
        "{}",
        serde_json::json!({"samples":costs.len(),"readySamples":successes,"statusUs":{"median":pct(&mut costs,0.5),"p95":pct(&mut costs,0.95)},"nativeCaptureUs":frame.capture_us,"geometryReady":frame.navigation.geometry_ready,"obstacles":frame.navigation.obstacles.len(),"portals":frame.navigation.portals.len(),"navigationMap":frame.navigation.map_id})
    );
    assert!(
        frame.navigation.geometry_ready,
        "obstacle geometry was not delivered to GameClient"
    );
}
