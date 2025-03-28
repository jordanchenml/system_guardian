#!/usr/bin/env python3
"""
檢查incidents和events表之間的關聯狀態
"""

import asyncio
import asyncpg
from system_guardian.settings import settings


async def check_incidents_and_events():
    """檢查incidents和events表的關聯狀態"""
    connection_string = str(settings.db_url).replace("postgresql+asyncpg", "postgresql")
    print(f"使用連接字符串: {connection_string}")

    try:
        # 連接數據庫
        connection = await asyncpg.connect(connection_string)

        # 檢查incidents表
        print("\n=== Incidents表情況 ===")
        incidents = await connection.fetch(
            "SELECT id, title, trigger_event_id FROM incidents ORDER BY id"
        )
        print(f"總共有 {len(incidents)} 個incidents")

        for incident in incidents:
            trigger_id = incident["trigger_event_id"]
            status = "有設置" if trigger_id is not None else "沒有設置"
            title = incident["title"]
            if len(title) > 40:
                title = title[:40] + "..."
            print(f"Incident #{incident['id']}: '{title}'")
            print(f"  trigger_event_id: {trigger_id} ({status})")

            # 如果有trigger_event_id，檢查對應的event是否存在
            if trigger_id is not None:
                event = await connection.fetchrow(
                    "SELECT id, source, event_type FROM events WHERE id = $1",
                    trigger_id,
                )
                if event:
                    print(
                        f"  對應的事件存在: Event #{event['id']} - {event['source']}/{event['event_type']}"
                    )
                else:
                    print(f"  ⚠️ 對應的事件不存在！")

        # 檢查events與incidents的關聯
        print("\n=== Events表情況 ===")
        events = await connection.fetch(
            """
            SELECT e.id, e.source, e.event_type, e.related_incident_id,
                   i.id as related_incident_id, i.trigger_event_id
            FROM events e
            LEFT JOIN incidents i ON e.id = i.trigger_event_id
            ORDER BY e.id
            LIMIT 20
            """
        )
        total_events = await connection.fetchval("SELECT COUNT(*) FROM events")
        print(f"總共有 {total_events} 個events (僅顯示前20個)")

        trigger_count = 0
        for event in events:
            event_id = event["id"]
            related_incident_id = event["related_incident_id"]
            is_trigger = event["related_incident_id"]

            # 基本資訊
            print(f"Event #{event_id} - {event['source']}/{event['event_type']}")

            # 是否與incident關聯
            if related_incident_id:
                print(f"  屬於 Incident #{related_incident_id}")
            else:
                print(f"  不屬於任何incident")

            # 是否為trigger event
            if is_trigger:
                trigger_count += 1
                print(f"  是 Incident #{is_trigger} 的trigger event ✓")
            else:
                print(f"  不是任何incident的trigger event")

        # 獲取總的trigger event數量
        total_trigger_count = await connection.fetchval(
            """
            SELECT COUNT(*) FROM events e
            JOIN incidents i ON e.id = i.trigger_event_id
            """
        )
        print(f"\n總結: {total_trigger_count}/{total_events} 的事件是trigger events")

        # 檢查不一致情況
        print("\n=== 檢查不一致情況 ===")
        inconsistencies = await connection.fetch(
            """
            SELECT i.id, i.title, i.trigger_event_id, e.id AS event_id
            FROM incidents i
            LEFT JOIN events e ON i.trigger_event_id = e.id
            WHERE i.trigger_event_id IS NOT NULL AND e.id IS NULL
            """
        )

        if inconsistencies:
            print(
                f"找到 {len(inconsistencies)} 個不一致情況(trigger_event_id指向不存在的event)"
            )
            for row in inconsistencies:
                title = row["title"]
                if len(title) > 40:
                    title = title[:40] + "..."
                print(f"Incident #{row['id']} '{title}'")
                print(f"  trigger_event_id={row['trigger_event_id']} 指向不存在的事件")
        else:
            print("未發現不一致情況")

        # 關閉連接
        await connection.close()

    except Exception as e:
        print(f"錯誤: {str(e)}")


if __name__ == "__main__":
    asyncio.run(check_incidents_and_events())
