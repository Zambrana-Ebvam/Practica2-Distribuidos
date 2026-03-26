import json
import secrets

def generar_llaves():
    # Diccionario con los algoritmos reales del PDF
    llaves = {
        "1":  {"banco": "Union", "alg": "Cesar", "shift": 5},
        "2":  {"banco": "Mercantil", "alg": "Atbash", "key": None}, # Cambiado de null a None
        "3":  {"banco": "BNB", "alg": "Vigenere", "key": "BOLIVIA"},
        "4":  {"banco": "BCP", "alg": "Playfair", "key": "MONEDA"},
        "5":  {"banco": "BISA", "alg": "Hill", "matrix": [[3, 3], [2, 5]]},
        "6":  {"banco": "Ganadero", "alg": "DES", "key": "8bytekey"},
        "7":  {"banco": "Economico", "alg": "3DES", "key": "16bytekey_for_3des_"},
        "8":  {"banco": "Prodem", "alg": "Blowfish", "key": "secret_key_prodem"},
        "9":  {"banco": "Solidario", "alg": "Twofish", "key": "solidario_key_32"},
        "10": {"banco": "Fortaleza", "alg": "AES", "key": "fortaleza_aes_16"},
        "11": {"banco": "FIE", "alg": "RSA", "key": "asfi_rsa_pair"},
        "12": {"banco": "Comunidad", "alg": "ElGamal", "key": "elgamal_key"},
        "13": {"banco": "Des_Productivo", "alg": "ECC", "key": "ecc_curve_p256"},
        "14": {"banco": "Nacion_Arg", "alg": "ChaCha20", "key": "nacion_arg_chacha20_32bytes_key"}
    }
    
    # Guardar en archivo JSON
    try:
        with open('asfi_keys.json', 'w') as f:
            json.dump(llaves, f, indent=4)
        print("✅ Archivo 'asfi_keys.json' creado con éxito.")
        print("📍 Ubicación:", f"{__file__.replace('setup_asfi.py', '')}asfi_keys.json")
    except Exception as e:
        print(f"❌ Error al crear el archivo: {e}")

if __name__ == "__main__":
    generar_llaves()