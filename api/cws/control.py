from .common import CWSContext


async def handle_control_opcode(ctx: CWSContext, msg: dict, opcode: str | None, req_guid: str | None) -> bool:
    if opcode == "authorize":
        auth_guid = msg.get("guid") or req_guid
        await ctx.safe_send_json(
            {
                "requestGuid": auth_guid,
                "httpCode": 200,
                "message": "The connection has been initialized.",
            }
        )
        return True

    if opcode == "ping":
        ping_guid = msg.get("guid")
        await ctx.safe_send_json(
            {
                "opcode": "ping",
                "guid": ping_guid,
                "confirm": True,
            }
        )
        return True

    return False
