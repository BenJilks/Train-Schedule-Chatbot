import config
from typing import Callable
from dataclasses import dataclass
from mastodon import Mastodon, CallbackStreamListener

@dataclass
class Message:
    id: int
    in_reply_to: int
    text: str
    from_user: str

@dataclass
class ConversationState:
    count: int = 0

def open_bot() -> Mastodon:
    return Mastodon(
        access_token = config.MASTODON_ACCESS_TOKEN,
        api_base_url = config.MASTODON_API_BASE_URL)

def send_reply(mastodon: Mastodon, 
               to: Message, text: str) -> Message:
    response = mastodon.status_post(
        f'@{ to.from_user } { text }',
        in_reply_to_id=to.id)
    assert isinstance(response, dict)

    response_id = response['id']
    assert isinstance(response_id, int)

    return Message(response_id, to.id, text, 'trainbot')

def listen_for_messages(mastodon: Mastodon,
                        respond_to_message: Callable[[Message], None]):
    def handle_notification(notification):
        notification_type = notification['type']
        if notification_type != 'mention':
            return

        status = notification['status']
        message = Message(
            id = status['id'],
            in_reply_to = status['in_reply_to_id'],
            text = status['content'],
            from_user = status['account']['username'])
        
        respond_to_message(message)

    listner = CallbackStreamListener(
        notification_handler = lambda x: handle_notification(x))
    mastodon.stream_user(listner)

def conversation_handler(mastodon: Mastodon,
                         state_type: type[ConversationState],
                         handle_conversation_state,
                         application_state = []):
    conversation_states: dict[int, ConversationState] = {}

    def respond_to_message(message: Message) -> None:
        state = state_type()
        if message.in_reply_to in conversation_states:
            state = conversation_states.pop(message.in_reply_to)

        response = handle_conversation_state(mastodon, message, state,
            *application_state)
        state.count += 1

        if not response is None:
            conversation_states[response.id] = state

    listen_for_messages(mastodon, respond_to_message)

