from typing import Optional, Dict, Any
import json
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, Request

from system_guardian.web.api.ingest.schema import Message

from loguru import logger

router = APIRouter()


@router.post("/github", response_model=Message)
async def process_github_webhook(
    request: Request,
    x_github_event: Optional[str] = Header(None, alias="X-GitHub-Event"),
) -> Message:
    """
    Process GitHub webhook events.

    This endpoint accepts GitHub webhook payloads for various events (PRs, Issues, Deployments, etc.)
    and processes them according to the event type specified in the X-GitHub-Event header.

    :param request: The incoming request object
    :param x_github_event: GitHub event type from X-GitHub-Event header
    :returns: message indicating successful processing
    """
    # Here you would typically validate the webhook signature
    # and process the event according to its type
    
    # Log event type for debugging
    logger.info(f"GitHub Event: {x_github_event}")
    content_type = request.headers.get("content-type", "")
    
    # 處理不同格式的請求數據
    if "application/json" in content_type:
        # 直接解析 JSON
        body = await request.json()
    else:
        # 處理表單數據
        form_data = await request.body()
        form_data_str = form_data.decode('utf-8')
        
        try:
            # 嘗試解析表單數據
            parsed_data = parse_qs(form_data_str)
            
            # GitHub webhook 通常在 'payload' 字段中包含 JSON 數據
            if 'payload' in parsed_data:
                payload_json = parsed_data['payload'][0]
                body = json.loads(payload_json)
            else:
                # 如果沒有 payload 字段，可能整個請求體就是 JSON
                body = json.loads(form_data_str)
        except Exception as e:
            print(f"Error parsing request data: {e}")
            # 返回錯誤訊息作為回應
            return Message(message=f"Error processing webhook: {str(e)}")
    
    # 打印 body 的內容
    logger.debug(f"Received body: {body}")
    
    # 返回處理後的數據
    return Message(message=body)
