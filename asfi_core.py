import base64
import json
from Crypto.Cipher import AES, DES, DES3, Blowfish
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Util.Padding import unpad

# --- 1. CARGAR LLAVES DE LA ASFI ---
# La ASFI tiene acceso a este archivo que le dieron los bancos
try:
    with open('asfi_keys.json', 'r') as f:
        keys = json.load(f)
except FileNotFoundError:
    print("❌ ERROR CRÍTICO: La ASFI no encuentra las llaves criptográficas (asfi_keys.json).")
    keys = {}

# --- 2. FUNCIONES DE DESCIFRADO ---

# -- Clásicos --
def dec_cesar(txt, shift=5):
    if txt == "ERR_CIFRADO": return 0.0
    return "".join([chr(ord(c) - shift) for c in str(txt)])

def dec_atbash(txt):
    if txt == "ERR_CIFRADO": return 0.0
    chars = "0123456789.-ABCDEFGHIJKL"
    rev = chars[::-1]
    try:
        # Atbash es simétrico por naturaleza, la función de cifrado es la misma que descifrado
        return str(txt).translate(str.maketrans(chars, rev))
    except Exception:
        return 0.0

def dec_vigenere(txt, key="BOLIVIA"):
    if txt == "ERR_CIFRADO": return 0.0
    txt = str(txt)
    return "".join([chr((ord(txt[i]) - ord(key[i % len(key)])) % 256) for i in range(len(txt))])

# -- Simétricos de Bloque (DES, 3DES, AES, Blowfish) --
def dec_bloque(b64_txt, key_bytes, cipher_module):
    if b64_txt == "ERR_CIFRADO": return 0.0
    try:
        encrypted_bytes = base64.b64decode(b64_txt)
        cipher = cipher_module.new(key_bytes, cipher_module.MODE_ECB)
        decrypted_padded = cipher.decrypt(encrypted_bytes)
        return unpad(decrypted_padded, cipher_module.block_size).decode('utf-8')
    except Exception as e:
        print(f"Error descifrando bloque: {e}")
        return 0.0

# -- Mocks Simétricos (Playfair, Hill, Twofish) --
def dec_mock_base64(b64_txt, prefix=""):
    if b64_txt == "ERR_CIFRADO": return 0.0
    try:
        decrypted = base64.b64decode(b64_txt).decode('utf-8')
        if prefix and decrypted.startswith(prefix):
            return decrypted.replace(prefix, "")
        if prefix == "HILL": # Nuestro mock de Hill invertía el string
            return decrypted[::-1]
        return decrypted
    except Exception:
        return 0.0

# --- 3. EL CEREBRO DESENCRIPTADOR ---
def asfi_descifrar_saldo(id_banco, saldo_cifrado):
    """
    Esta función recibe el ID del banco y el saldo cifrado, 
    busca la llave correcta y devuelve el saldo original en USD.
    """
    bid = str(id_banco)
    
    # --- PARCHE PARA NEO4J (TEXTO PLANO) ---
    # Si es el banco 14, lo dejamos pasar como número directo sin descifrar
    if bid == "14":
        try:
            return float(saldo_cifrado)
        except Exception:
            return 0.0

    try:
        if bid == "1": return float(dec_cesar(saldo_cifrado, keys.get("1", {}).get("shift", 5)))
        elif bid == "2": return float(dec_atbash(saldo_cifrado))
        elif bid == "3": return float(dec_vigenere(saldo_cifrado, keys.get("3", {}).get("key", "BOLIVIA")))
        elif bid == "4": return float(dec_mock_base64(saldo_cifrado)) # Playfair Mock
        elif bid == "5": return float(dec_mock_base64(saldo_cifrado, prefix="HILL")) # Hill Mock
        elif bid == "6": return float(dec_bloque(saldo_cifrado, b'8bytekey', DES))
        elif bid == "7": return float(dec_bloque(saldo_cifrado, b'16bytekey_3des__', DES3))
        elif bid == "8": return float(dec_bloque(saldo_cifrado, b'secret_key_prodem', Blowfish))
        elif bid == "9": return float(dec_mock_base64(saldo_cifrado, prefix="TWOFISH_"))
        elif bid == "10": return float(dec_bloque(saldo_cifrado, b'fortaleza_aes_16', AES))
        elif bid == "11": return float(dec_mock_base64(saldo_cifrado)) # MOCK TEMPORAL PARA RSA
        elif bid == "12": return float(dec_mock_base64(saldo_cifrado, prefix="ELGAMAL_"))
        elif bid == "13": return float(dec_mock_base64(saldo_cifrado, prefix="ECC_"))
        else: return 0.0
    except Exception as e:
        # Si algo falla radicalmente, retornamos 0 para no tumbar la ASFI
        # print(f"Error crítico descifrando banco {bid}: {e}")
        return 0.0

# --- PRUEBA RÁPIDA ---
if __name__ == "__main__":
    print("Probando el motor de descifrado de la ASFI...")
    
    # Prueba con Banco 2 (Atbash)
    saldo_prueba_cifrado = dec_atbash("100.50")
    print(f"Saldo original: 100.50 | Cifrado Atbash: {saldo_prueba_cifrado}")
    print(f"La ASFI descifró: {asfi_descifrar_saldo(2, saldo_prueba_cifrado)}")
    
    # Prueba con Banco 14 (Neo4j Texto Plano)
    print(f"\nPrueba Banco 14 (Neo4j) enviando '550.75' sin cifrar:")
    print(f"La ASFI leyó: {asfi_descifrar_saldo(14, '550.75')}")