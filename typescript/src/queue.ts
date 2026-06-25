export interface Message {
  readonly body: string;
}

export interface Queue {
  putMessage(topic: string, body: string): Promise<void>;

  getMessage(topic: string): Promise<Message>;
}
