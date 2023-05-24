# --------------------------------------------------------------------------- #
# Modbus Solarmanv5 Message
# --------------------------------------------------------------------------- #

from pymodbus.framer import BYTE_ORDER, FRAME_HEADER, ModbusFramer
from pymodbus.framer.rtu_framer import ModbusRtuFramer

import struct


class ModbusSolarmanv5Framer(ModbusFramer):
    """Modbus Solarmanv5 Frame controller.

    The Solarmanv5 protocol is a proprietary protocol that is used by solar
    inverter data loggers. The protocol is warping a normal modbus RTU with a
    header, payload(containing the RTU) and a trailer.

    [header][payload][trailer]
       11b       Nb        2b


    Header format:
    [Start 0xA5][PayloadLength][ControlCode][Serial][LoggerSerialNr]
        1b           2b           2b           2b           4b

    Trailer format:
    [Checksum][End 0x15]
      1b         1b

    Request payload format:

    [FrameType][SensorType][TotalWorkingTime][PowerOnTime][OffsetTime][RTU]
        1b          2b           4b               4b           4b      Nb

    See https://pysolarmanv5.readthedocs.io/en/latest/solarmanv5_protocol.html for more information.

    """

    method = "solarmanv5"

    def __init__(self, decoder, client=None):
        """Initialize a new instance of the framer.

        :param decoder: The decoder factory implementation to use
        """
        self._buffer = b""
        self._start = b"\xa5"
        self._end = b"\x15"
        self._controlcode = struct.pack("<H", 0x4510)
        self._payload = bytearray(
            b"\x02" +                        # FrameType
            struct.pack("<H", 0x0000) +      # SensorType
            struct.pack("<I", 0x00000000) +  # TotalWorkingTime
            struct.pack("<I", 0x00000000) +  # PowerOnTime
            struct.pack("<I", 0x00000000)    # OffsetTime
        )
        self._trailer = bytearray(
            b"\x00" +    # Checksum
            self._end    # End
        )
        self.decoder = decoder
        self.client = client
        self.rtuFramer = ModbusRtuFramer(decoder, client)


    def checkFrame(self):  # pylint: disable=invalid-name
        """Check and decode the next frame.

        :returns: True if we successful, False otherwise
        """
        buffer_length = len(self._buffer)

        if buffer_length < 11 + 15 + 2:
            return False
        start = self._buffer.find(self._start)
        if start == -1:
            return False
        payload_length = struct.unpack("<H", self._buffer[start + 1:start + 3])[0]
        if buffer_length < start + 15 + payload_length + 2:
            return False
        if self._buffer[start + 15 + payload_length + 1] != self._end:
            return False
        return True

    def advanceFrame(self):  # pylint: disable=invalid-name
        """Skip over the current framed message.

        This allows us to skip over the current message after we have processed
        it or determined that it contains an error. It also has to reset the
        current frame header handle
        """
        raise NotImplementedException(TEXT_METHOD)

    def addToFrame(self, message):  # pylint: disable=invalid-name
        """Add the next message to the frame buffer.

        This should be used before the decoding while loop to add the received
        data to the buffer handle.

        :param message: The most recent packet
        :raises NotImplementedException:
        """
        self._buffer += message

    def isFrameReady(self):  # pylint: disable=invalid-name
        """Check if we should continue decode logic.

        This is meant to be used in a while loop in the decoding phase to let
        the decoder know that there is still data in the buffer.
        """
        return len(self._buffer) > 1

    def getFrame(self):  # pylint: disable=invalid-name
        """Get the next frame from the buffer.

        :raises NotImplementedException:
        """
        raise NotImplementedException(TEXT_METHOD)

    def populateResult(self, result):  # pylint: disable=invalid-name
        """Populate the modbus result with current frame header.

        We basically copy the data back over from the current header
        to the result header. This may not be needed for serial messages.

        :param result: The response packet
        :raises NotImplementedException:
        """
        raise NotImplementedException(TEXT_METHOD)

    def processIncomingPacket(self, data, callback, unit, **kwargs):  # pylint: disable=invalid-name
        """Process new packet pattern.

        This takes in a new request packet, adds it to the current
        packet stream, and performs framing on it. That is, checks
        for complete messages, and once found, will process all that
        exist.  This handles the case when we read N + 1 or 1 / N
        messages at a time instead of 1.

        The processed and decoded messages are pushed to the callback
        function to process and send.

        :param data: The new packet data
        :param callback: The function to send results to
        :raises NotImplementedException:
        """
        if not isinstance(unit, (list, tuple)):
            unit = [unit]
        self.addToFrame(data)
        single = kwargs.get("single", False)
        while True:
            if self.isFrameReady():
                if self.checkFrame():
                    if self._validate_unit_id(unit, single):
                        self._process(callback)
                    else:
                        header_txt = self._header["uid"]
                        Log.debug("Not a valid unit id - {}, ignoring!!", header_txt)
                        self.resetFrame()
                        break
                else:
                    #Log.debug("Frame check failed, ignoring!!")
                    self.resetFrame()
                    break
            else:
                #Log.debug("Frame - [{}] not ready", data)
                break

    def _process(self, callback, error=False):
        """Process incoming packets irrespective error condition."""
        data = self._buffer if error else self.getFrame()
        if (result := self.decoder.decode(data)) is None:
            raise ModbusIOException("Unable to decode request")
        if error and result.function_code < 0x80:
            raise InvalidMessageReceivedException(result)
        self.populateResult(result)
        self.advanceFrame()
        callback(result)  # defer or push to a thread?


    def buildPacket(self, message):  # pylint: disable=invalid-name
            """Create a ready to send modbus packet.

            The raw packet is built off of a fully populated modbus
            request / response message.

            :param message: The request/response to send
            :raises NotImplementedException:
            """

            # We only support requests
            encoded = self.rtuFramer.buildPacket(message)

            payload_length = 15 + len(encoded)

            header = bytearray(
                self._start +
                struct.pack("<H", payload_length) +
                self._controlcode +
                struct.pack("<H", 0x0001) +
                struct.pack("<I", 2722590612)
            )

            frame = header + self._payload + encoded + self._trailer
            frame[len(frame) - 2] = self._checksum(frame)
            return frame

    @staticmethod
    def _checksum(frame):
        checksum = 0
        for i in range(1, len(frame) - 2, 1):
            checksum += frame[i] & 0xFF
        return int(checksum & 0xFF)
