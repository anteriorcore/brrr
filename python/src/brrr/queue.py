from abc import ABC, abstractmethod
from dataclasses import dataclass


class QueueIsEmpty(Exception):
    pass


class QueueIsClosed(Exception):
    """
    Queue implementations may raise this exception for workers to shut down gracefully
    """


@dataclass
class Message:
    """Result of a queue Read operation.

    body is the raw payload.  Not bytes; it is up to the codec to actually
    respect this, but the assumption is that the message payload is encodable as
    printable characters, and compatible with the Queue implementation.  This is
    not _strictly_ speaking a requirement by brrr lib, because brrr lib doesn't
    care, as long as your queue is able to handle it: the user brings both the
    codec and the queue implementation, so it "should be fine".  However, you
    almost certainly want printable characters here, so we made the type `str`
    rather than bytes.

    Receipt_handle is like SQS.  It is of course up to the actual queue
    implementation to support this, but if it wants to, it can.  This makes brrr
    at-least-once-delivery, instead of zero-or-more.

    """

    body: str
    receipt_handle: str | None


@dataclass
class QueueInfo:
    """
    Approximate info about a queue.

    Vestigial and purely  best effort at this point.
    """

    num_messages: int


# Infra abstractions


class Queue(ABC):
    """The queue backing brrr must satisfy this interface.

    Required semantics:
    - topics
    - string message payloads

    Not required:
    - at-least-once delivery guarantees
    - at-most-once delivery guarantees
    - receipt handle support
    - order

    As with the Store interface, consistency and availability guarantees are up
    to the implementation of this interface, and they will transparently bubble
    up, through brrr, to the application.  The implementation can choose to
    implement visibility timeouts, retries, and dead-lettering, or it can choose
    to forego all of that and be zero-or-more delivery.  Brrr will be fine; your
    application will have to handle the consequences.

    The recv_block_secs is inspired by SQS: maximum time for a get_message call
    to block while waiting for new messages, if there are currently no messages
    on the queue.  Just as with SQS, if there is a message, return immediately.
    This value is best effort, just a way to keep this DRY if nothing else.  If
    the underlying queue doesn’t support this primitive it’s OK, but don’t sleep
    for this time indiscriminately because the point of this value is to block
    only while there are no messages available.

    Bare SQS _could_ satisfy this interface if you had only one single topic.

    """

    recv_block_secs: int = 20

    throws_queue_is_closed: bool
    has_accurate_info: bool
    deletes_messages: bool

    @abstractmethod
    async def put_message(self, topic: str, body: str) -> None: ...
    @abstractmethod
    async def get_message(self, topic: str) -> Message: ...

    async def delete_message(self, topic: str, receipt_handle: str) -> None:
        # Optional
        pass
