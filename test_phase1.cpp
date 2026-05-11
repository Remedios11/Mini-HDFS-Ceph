#include "db.h"
#include <cassert>
#include <iostream>
#include <string>
#include <filesystem>

namespace fs = std::filesystem;

int main() {
    std::cout << "=== Phase 1: 单机存储引擎测试 ===\n\n";

    const std::string db_path = "/tmp/test_phase1_db";
    fs::remove_all(db_path);

    mini_storage::Options options;
    options.db_path = db_path;

    mini_storage::DB db(options);
    std::cout << "✓ DB 构造成功\n";

    assert(db.Put("name", "Alice"));
    assert(db.Put("age",  "18"));
    assert(db.Put("city", "Beijing"));
    std::cout << "✓ Put 3条记录\n";

    std::string val;
    assert(db.Get("name", &val) && val == "Alice");
    assert(db.Get("age",  &val) && val == "18");
    assert(db.Get("city", &val) && val == "Beijing");
    std::cout << "✓ Get 3条记录，内容正确\n";

    assert(db.Put("age", "20"));
    assert(db.Get("age", &val) && val == "20");
    std::cout << "✓ 覆盖写（age: 18 → 20）\n";

    assert(db.Delete("city"));
    assert(!db.Get("city", &val));
    std::cout << "✓ Delete 后 Get 返回 false\n";

    assert(!db.Get("nonexistent_key_xyz", &val));
    std::cout << "✓ 不存在的 key 返回 false\n";

    for (int i = 0; i < 1000; i++) {
        assert(db.Put("key_" + std::to_string(i), "value_" + std::to_string(i)));
    }
    std::cout << "✓ 批量写入 1000 条\n";

    for (int i : {0, 100, 500, 999}) {
        assert(db.Get("key_" + std::to_string(i), &val));
        assert(val == "value_" + std::to_string(i));
    }
    std::cout << "✓ 抽检通过\n";

    db.FlushMemTable();
    std::cout << "✓ FlushMemTable 成功\n";

    assert(db.Get("name", &val) && val == "Alice");
    std::cout << "✓ 刷盘后读取正确\n";

    {
        mini_storage::DB db2(options);
        assert(db2.Get("name", &val) && val == "Alice");
        assert(!db2.Get("city", &val));
        std::cout << "✓ 重新打开后数据持久化正常\n";
    }

    std::cout << "\n✅ Phase 1 所有测试通过！\n";
    return 0;
}
