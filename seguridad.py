import base64
from Crypto.Cipher import AES, DES
from Crypto.Util.Padding import pad
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

# --- CLÁSICOS ---
def enc_cesar(txt, s=5):
    return "".join([chr(ord(c) + s) for c in str(txt)])

def enc_atbash(txt):
    # Diccionario adaptado para números y decimales
    chars = "0123456789.ABCDEFGHIJKLM" 
    rev = chars[::-1]
    try:
        return str(txt).translate(str.maketrans(chars, rev))
    except:
        return str(txt)

def enc_vigenere(txt, key="BOLIVIA"):
    txt = str(txt)
    return "".join([chr((ord(txt[i]) + ord(key[i % len(key)])) % 256) for i in range(len(txt))])

# --- SIMÉTRICOS MODERNOS ---
def enc_des(txt, key=b'8bytekey'): # La llave de DES DEBE ser de 8 bytes
    cipher = DES.new(key, DES.MODE_ECB)
    padded_txt = pad(str(txt).encode(), DES.block_size)
    return base64.b64encode(cipher.encrypt(padded_txt)).decode()

def enc_aes(txt, key=b'fortaleza_aes_16'): # La llave de AES puede ser de 16, 24 o 32 bytes
    cipher = AES.new(key, AES.MODE_ECB) 
    padded_txt = pad(str(txt).encode(), AES.block_size)
    return base64.b64encode(cipher.encrypt(padded_txt)).decode()

# --- ASIMÉTRICOS ---
# Generamos la llave RSA una sola vez fuera de la función para no destruir el CPU en el loop
rsa_key = RSA.generate(1024)
rsa_cipher = PKCS1_OAEP.new(rsa_key.publickey())

def enc_rsa(txt):
    encrypted = rsa_cipher.encrypt(str(txt).encode())
    return base64.b64encode(encrypted).decode()