def is_saved_messages_dialog(dialog) -> bool:
    entity = getattr(dialog, 'entity', None)
    return bool(getattr(dialog, 'is_user', False) and getattr(entity, 'self', False))


def is_selectable_target_dialog(dialog, include_saved_messages: bool = False) -> bool:
    if getattr(dialog, 'is_group', False) or getattr(dialog, 'is_channel', False):
        return True
    return include_saved_messages and is_saved_messages_dialog(dialog)


def get_dialog_display_name(dialog) -> str:
    if is_saved_messages_dialog(dialog):
        return "Saved Messages"
    return getattr(dialog, 'name', None) or "Unknown Chat"


def resolve_chat_peer(client, chat_id: int):
    me = getattr(client, 'me', None)
    if me and chat_id == getattr(me, 'id', None):
        return 'me'
    return chat_id
