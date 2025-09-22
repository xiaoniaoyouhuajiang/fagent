#!/usr/bin/env bash

# 加载环境变量
set -a
source .env
set +a

echo "=== GitFetcher 实际功能测试 ==="
echo "测试时间: $(date)"
echo "GitHub Token: ${GITHUB_TOKEN:0:10}... (已设置)"
echo ""

# 创建输出目录
OUTPUT_DIR="./test_output"
mkdir -p "$OUTPUT_DIR"

# 测试一个小的公开仓库
OWNER="octocat"
REPO="Hello-World"

echo "🔍 测试仓库: $OWNER/$REPO"
echo ""

# 构建项目
echo "📦 构建项目..."
cargo build --release
if [ $? -ne 0 ]; then
    echo "❌ 构建失败"
    exit 1
fi
echo "✅ 构建成功"
echo ""

# 测试JSON格式输出
echo "📄 测试JSON格式输出..."
rm -rf "$OUTPUT_DIR"/*
cargo run --release -- "$OWNER" "$REPO" json core "$OUTPUT_DIR"
if [ $? -eq 0 ]; then
    echo "✅ JSON输出成功"
    
    # 检查输出文件
    if [ -f "$OUTPUT_DIR/issues.json" ]; then
        echo "   📊 Issues文件大小: $(du -h "$OUTPUT_DIR/issues.json" | cut -f1)"
        echo "   📋 Issues数量: $(jq '. | length' "$OUTPUT_DIR/issues.json" 2>/dev/null || echo '解析失败')"
    fi
    
    if [ -f "$OUTPUT_DIR/pull_requests.json" ]; then
        echo "   🔄 PR文件大小: $(du -h "$OUTPUT_DIR/pull_requests.json" | cut -f1)"
        echo "   📋 PR数量: $(jq '. | length' "$OUTPUT_DIR/pull_requests.json" 2>/dev/null || echo '解析失败')"
    fi
    
    if [ -f "$OUTPUT_DIR/commits.json" ]; then
        echo "   💾 Commits文件大小: $(du -h "$OUTPUT_DIR/commits.json" | cut -f1)"
        echo "   📋 Commit数量: $(jq '. | length' "$OUTPUT_DIR/commits.json" 2>/dev/null || echo '解析失败')"
    fi
else
    echo "❌ JSON输出失败"
fi
echo ""

# 测试CSV格式输出
echo "📊 测试CSV格式输出..."
rm -rf "$OUTPUT_DIR"/*
cargo run --release -- "$OWNER" "$REPO" csv core "$OUTPUT_DIR"
if [ $? -eq 0 ]; then
    echo "✅ CSV输出成功"
    
    # 检查输出文件
    if [ -f "$OUTPUT_DIR/issues.csv" ]; then
        echo "   📊 Issues文件大小: $(du -h "$OUTPUT_DIR/issues.csv" | cut -f1)"
        echo "   📋 Issues数量: $(($(wc -l < "$OUTPUT_DIR/issues.csv") - 1))"
    fi
    
    if [ -f "$OUTPUT_DIR/pull_requests.csv" ]; then
        echo "   🔄 PR文件大小: $(du -h "$OUTPUT_DIR/pull_requests.csv" | cut -f1)"
        echo "   📋 PR数量: $(($(wc -l < "$OUTPUT_DIR/pull_requests.csv") - 1))"
    fi
    
    if [ -f "$OUTPUT_DIR/commits.csv" ]; then
        echo "   💾 Commits文件大小: $(du -h "$OUTPUT_DIR/commits.csv" | cut -f1)"
        echo "   📋 Commit数量: $(($(wc -l < "$OUTPUT_DIR/commits.csv") - 1))"
    fi
else
    echo "❌ CSV输出失败"
fi
echo ""

# 显示输出文件列表
echo "📁 生成的文件:"
ls -la "$OUTPUT_DIR" 2>/dev/null || echo "   (没有生成文件)"
echo ""

# 显示文件内容示例
echo "📋 文件内容示例:"
if [ -f "$OUTPUT_DIR/issues.json" ]; then
    echo "   issues.json (前5行):"
    head -5 "$OUTPUT_DIR/issues.json"
    echo ""
fi

if [ -f "$OUTPUT_DIR/issues.csv" ]; then
    echo "   issues.csv (前5行):"
    head -5 "$OUTPUT_DIR/issues.csv"
    echo ""
fi

echo "=== 测试完成 ==="