fn main() {
    std::process::exit(xenomit_botd::cli::run(std::env::args().skip(1)));
}
