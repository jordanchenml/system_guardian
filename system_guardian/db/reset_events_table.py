#!/usr/bin/env python3
"""
重置events表，解決事件插入失敗問題
此腳本將完全重建events表結構
"""

import asyncio
import asyncpg
from loguru import logger
from system_guardian.settings import settings


async def reset_events_table():
    """完全重建events表，修復可能的結構問題"""
    connection_string = str(settings.db_url).replace("postgresql+asyncpg", "postgresql")
    print(f"連接到數據庫: {connection_string}")

    try:
        # 連接數據庫
        connection = await asyncpg.connect(connection_string)

        # 檢查events表是否存在
        exists = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'events'
            )
            """
        )

        if exists:
            print("發現現有events表，準備備份和重建")

            # 備份表結構
            schema = await connection.fetch(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'events'
                ORDER BY ordinal_position
                """
            )

            print(f"當前表結構: {', '.join([col['column_name'] for col in schema])}")

            # 嘗試備份數據（如果有的話）
            try:
                backup_count = await connection.fetchval("SELECT COUNT(*) FROM events")
                if backup_count > 0:
                    print(f"找到 {backup_count} 筆事件數據，準備備份")

                    # 創建臨時備份表
                    await connection.execute(
                        """
                        CREATE TABLE IF NOT EXISTS events_backup AS 
                        SELECT * FROM events
                        """
                    )

                    backup_verify = await connection.fetchval(
                        "SELECT COUNT(*) FROM events_backup"
                    )
                    print(f"備份了 {backup_verify} 筆事件數據到 events_backup 表")
                else:
                    print("events表中沒有數據，無需備份")
            except Exception as backup_err:
                print(f"備份數據時出錯: {str(backup_err)}")
                print("繼續重建表結構")

            # 移除外鍵約束
            print("移除外鍵約束...")
            await connection.execute(
                """
                DO $$
                BEGIN
                    -- 嘗試移除現有約束（如果存在）
                    BEGIN
                        ALTER TABLE events DROP CONSTRAINT IF EXISTS events_related_incident_id_fkey;
                    EXCEPTION WHEN OTHERS THEN
                        RAISE NOTICE 'Error removing related_incident_id constraint: %', SQLERRM;
                    END;
                    
                    BEGIN
                        ALTER TABLE events DROP CONSTRAINT IF EXISTS events_incident_id_fkey;
                    EXCEPTION WHEN OTHERS THEN
                        RAISE NOTICE 'Error removing incident_id constraint: %', SQLERRM;
                    END;
                    
                    -- 嘗試移除任何可能被引用的約束
                    BEGIN
                        ALTER TABLE incidents DROP CONSTRAINT IF EXISTS incidents_trigger_event_id_fkey;
                    EXCEPTION WHEN OTHERS THEN
                        RAISE NOTICE 'Error removing trigger_event_id constraint: %', SQLERRM;
                    END;
                END $$;
                """
            )

            # 刪除表
            print("刪除events表...")
            await connection.execute("DROP TABLE IF EXISTS events CASCADE")
            print("events表已刪除")

        # 創建全新的events表
        print("創建新的events表...")
        await connection.execute(
            """
            CREATE TABLE events (
                id SERIAL PRIMARY KEY,
                related_incident_id INTEGER NULL,
                source VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                content JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
            """
        )
        print("新events表創建成功")

        # 添加外鍵約束
        print("添加外鍵約束...")
        await connection.execute(
            """
            ALTER TABLE events 
            ADD CONSTRAINT events_related_incident_id_fkey 
            FOREIGN KEY (related_incident_id) 
            REFERENCES incidents(id) ON DELETE SET NULL
            """
        )
        print("外鍵約束添加成功")

        # 恢復備份數據（如果有的話）
        try:
            backup_exists = await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'events_backup'
                )
                """
            )

            if backup_exists:
                backup_count = await connection.fetchval(
                    "SELECT COUNT(*) FROM events_backup"
                )

                if backup_count > 0:
                    print(f"從備份表恢復 {backup_count} 筆數據...")

                    # 從備份恢復數據，使用新的列名
                    await connection.execute(
                        """
                        INSERT INTO events (id, related_incident_id, source, event_type, content, created_at)
                        SELECT 
                            id, 
                            CASE 
                                WHEN related_incident_id IS NOT NULL THEN related_incident_id
                                WHEN incident_id IS NOT NULL THEN incident_id
                                ELSE NULL
                            END as related_incident_id,
                            source, 
                            event_type, 
                            content, 
                            created_at
                        FROM events_backup
                        """
                    )

                    restored = await connection.fetchval("SELECT COUNT(*) FROM events")
                    print(f"成功恢復了 {restored} 筆數據")

                # 刪除備份表
                await connection.execute("DROP TABLE events_backup")
                print("備份表已刪除")
        except Exception as restore_err:
            print(f"恢復數據時出錯: {str(restore_err)}")

        # 修復序列編號
        print("修復自增序列...")
        await connection.execute(
            """
            SELECT setval('events_id_seq', COALESCE((SELECT MAX(id) FROM events), 0) + 1, false)
            """
        )

        # 驗證表結構
        schema = await connection.fetch(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'events'
            ORDER BY ordinal_position
            """
        )

        print("重建後的表結構:")
        for col in schema:
            print(
                f"- {col['column_name']} ({col['data_type']}, {'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL'})"
            )

        # 關閉連接
        await connection.close()
        print("表重建完成！")

    except Exception as e:
        print(f"重建表時出錯: {str(e)}")


if __name__ == "__main__":
    asyncio.run(reset_events_table())
