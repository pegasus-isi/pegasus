
class RecordParseException(Exception): pass

class Token:
    START = "["
    END = "]"
    VALUE = "<value>"
    EQUALS = "="
    COMMA = ","

class RecordParser(object):

    def __init__(self, string):
        self.string = string
        self.index = 0
        self.next = None

    def la(self):
        if self.index >= len(self.string):
            raise RecordParseException(
                    "Unexpected end of string", self.string)
        return self.string[self.index]

    def consume(self):
        self.index += 1

    def isvalue(self, c):
        "Is it a value character?"
        if c.isspace():
            return False
        if c in '[]=,"':
            return False
        return True

    def nextToken(self):
        "Get the next token"

        # This is for the one-token lookahead
        if self.next is not None:
            tok = self.next
            self.next = None
            return tok

        while self.index < len(self.string):
            if self.la() == "[":
                self.consume()
                return (Token.START, "[")
            elif self.la() == "]":
                self.consume()
                return (Token.END, "]")
            elif self.la() == ",":
                self.consume()
                return (Token.COMMA, ",")
            elif self.la() == "=":
                self.consume()
                return (Token.EQUALS, "=")
            elif self.la().isspace():
                # Whitespace
                self.consume()
            elif self.la() == '"':
                # It is a string value
                chars = []
                self.consume()
                while self.la() != '"':
                    chars.append(self.la())
                    self.consume()
                self.consume()
                return (Token.VALUE, "".join(chars))
            else:
                # It must be a regular value
                chars = []
                while self.isvalue(self.la()):
                    chars.append(self.la())
                    self.consume()
                return (Token.VALUE, "".join(chars))

        raise RecordParseException(
                "Unexpected end of record", self.string)

    def lt(self):
        "Look ahead one token"
        self.next = self.nextToken()
        return self.next[0]

    def expect(self, item):
        "Expect the next token to be item and return its value"
        token, value = self.nextToken()
        if token != item:
            raise RecordParseException(
                    "Expected '%s', got '%s'" % (item, token), self.string)
        return value

    def parse(self):
        "Parse a cluster record"
        VALID_TYPES = [
            "cluster-summary",
            "seqexec-summary",
            "cluster-task"
        ]

        self.expect(Token.START)
        rectype = self.expect(Token.VALUE)
        if rectype not in VALID_TYPES:
            raise RecordParseException(
                    "Invalid record type: %s" % rectype, self.string)
        record = {}

        while True:
            key = self.expect(Token.VALUE)
            self.expect(Token.EQUALS)
            value = self.expect(Token.VALUE)
            record[key] = value
            if self.lt() == Token.COMMA:
                self.expect(Token.COMMA)
            if self.lt() == Token.END:
                break
        self.expect(Token.END)

        return record

