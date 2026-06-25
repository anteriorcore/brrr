export interface Message {
  readonly body: string;
}

export type GetMessageReponse =
  | {
      closed: true;
    }
  | {
      closed: false;
      message: Message;
    }
  | undefined;

export interface Queue {
  putMessage(topic: string, body: string): Promise<void>;

  // NOMERGE somthign liek this
  getMessage(topic: string): Promise<GetMessageReponse>;
}
