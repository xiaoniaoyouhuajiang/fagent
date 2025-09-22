use gitfetcher::{run, OutputFormat, DetailLevel};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 4 {
        eprintln!("用法: {} <owner> <repo> <format> [level] [output_dir]", args[0]);
        eprintln!("示例: {} octocat Hello-World json core ./output", args[0]);
        eprintln!("格式: json, csv");
        eprintln!("级别: core, full");
        std::process::exit(1);
    }
    
    let owner = &args[1];
    let repo = &args[2];
    let format = match args[3].as_str() {
        "json" => OutputFormat::Json,
        "csv" => OutputFormat::Csv,
        _ => {
            eprintln!("错误: 不支持的格式 '{}'. 支持的格式: json, csv", args[3]);
            std::process::exit(1);
        }
    };
    
    let level = match args.get(4).map(|s| s.as_str()).unwrap_or("core") {
        "core" => DetailLevel::Core,
        "full" => DetailLevel::Full,
        _ => {
            eprintln!("错误: 不支持的级别 '{}'. 支持的级别: core, full", args[4]);
            std::process::exit(1);
        }
    };
    
    let output_dir = args.get(5).map(|s| s.as_str()).unwrap_or("./output");
    
    // 从环境变量获取GitHub token
    let token = env::var("GITHUB_TOKEN").ok();
    
    if token.is_none() {
        eprintln!("警告: 未设置 GITHUB_TOKEN 环境变量，可能会遇到API速率限制");
    }
    
    println!("开始获取仓库数据: {}/{}", owner, repo);
    println!("输出格式: {:?}", format);
    println!("详细级别: {:?}", level);
    println!("输出目录: {}", output_dir);
    
    match tokio::runtime::Runtime::new().unwrap().block_on(run(owner, repo, token, format, level, output_dir)) {
        Ok(()) => {
            println!("✅ 数据获取完成！");
        }
        Err(e) => {
            eprintln!("❌ 错误: {}", e);
            std::process::exit(1);
        }
    }
}