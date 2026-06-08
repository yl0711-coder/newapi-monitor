# 贡献指南

欢迎提 Issue 和 Pull Request。

## 本地开发
```bash
go test ./...    # 单元测试
go vet ./...     # 静态检查
gofmt -l .       # 检查格式(应无输出)
go build .       # 构建二进制
docker build -t newapi-monitor .   # 构建镜像
```

## 提交 PR
- 代码保持 `gofmt` 格式化(`gofmt -w .`);
- 新功能 / 修复请尽量附带测试;
- PR 会自动跑 CI(`go vet` + `go test`),通过后再合并。

## 约定
- 镜像由 CI 在 `main` 推送 / 打 `v*` tag 时自动构建并发布到 GHCR;
- **不要提交任何密钥 / `.env` / 数据库文件**(见 `.gitignore`)。
