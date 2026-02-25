from .common import WSContext


async def handle_control_opcode(ctx: WSContext, msg: dict, opcode: str | None, req_guid: str | None) -> bool:
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
        ping_guid = msg["guid"]
        ev = ctx.subs.get(ping_guid)
        is_alive = ev is not None and not ev.is_set()
        await ctx.safe_send_json(
            {
                "opcode": "ping",
                "guid": ping_guid,
                "confirm": is_alive,
            }
        )
        return True

    if opcode == "unsubscribe":
        unsub_guid = msg.get("guid") or req_guid
        ctx.unsubscribe_guid(unsub_guid)
        await ctx.safe_send_json(
            {
                "message": "Handled successfully",
                "httpCode": 200,
                "requestGuid": unsub_guid,
            }
        )
        return True

    return False
