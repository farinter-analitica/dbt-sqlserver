import base64

def encode_password(input_str: str) -> str:
    # Convert the string to bytes
    input_bytes = input_str.encode('utf-8')
    # Perform base64 encoding
    encoded_bytes = base64.urlsafe_b64encode(input_bytes)
    # Convert the bytes back to a string
    return encoded_bytes.decode('utf-8')

def decode_password(encoded_str: str) -> str:
    # Convert the encoded string back to bytes
    encoded_bytes = encoded_str.encode('utf-8')
    # Perform base64 decoding
    decoded_bytes = base64.urlsafe_b64decode(encoded_bytes)
    # Convert the bytes back to the original string
    return decoded_bytes.decode('utf-8')

# Example usage:
original_str = "!#$$^&^&((*^@!@32235masdasAASDsdd_<>??''23"
encoded_str = encode_password(original_str)
decoded_str = decode_password(encoded_str)

assert original_str == decoded_str, "The decoded string does not match the original."

print(f"Original: {original_str}")
print(f"Encoded: {encoded_str}")
print(f"Decoded: {decoded_str}")
