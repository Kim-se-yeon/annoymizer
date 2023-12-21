from pyspark.sql.functions import udf, concat
from pyspark.sql.types import StringType
from Crypto.Cipher import AES
from pyspark.sql.types import *
import pyspark.sql.functions as F
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import base64, hashlib, os
from dataframe_to_json import df_to_json


limit_value = 20

class OneWayEncryption():
    """
    | 일방향 암호화를 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_nane (str, list): 컬럼
        algorithm (str) : 알고리즘 종류 (SHA224, SHA256, SHA384, SHA512)
        encoding (str) : 암호화 될 결과값에 대한 인코딩 방법 (hex, base64)
        noise (str) : 잡음값 (사용자 입력)
        prefix (boolean) : 잡음값 위치 (앞)
        suffix (boolean) : 잡음값 위치 (뒤)
         * prefix, suffix 둘다 True 값 적용 가능
    """

    def __init__(self, input_df):
        self.input_df = input_df

    def apply_hash(self, column_name, algorithm, encoding, noise, prefix=False, suffix=False, preview='T'):
        def hash_all(value, algorithm, encoding, noise, prefix=False, suffix=False):
            if noise:
                value = "{0}{1}{2}".format(noise, value, noise)

            # Prefix, Suffix 중 하나 혹은 둘다 고르기
            if prefix and suffix:
                value = "{0}:{1}{2}{3}:{4}".format(algorithm, noise, value, noise, algorithm)
            elif prefix:
                value = "{0}:{1}{2}".format(algorithm, noise, value)
            elif suffix:
                value = "{0}{1}:{2}".format(noise, value, algorithm)

            # 알고리즘으로 해싱
            h = hashlib.new(algorithm)
            h.update(value.encode())
            result = h.digest()

            # 결과값 인코딩
            hash_object = hashlib.new(algorithm)
            hash_object.update(value.encode('utf-8'))
            if encoding == 'hex':
                return hash_object.hexdigest()
            elif encoding == 'base64':
                return base64.b64encode(hash_object.digest()).decode('utf-8')

        hash_all_udf = udf(lambda x: hash_all(value=x, algorithm=algorithm, encoding=encoding, noise=noise, prefix=prefix, suffix=suffix), StringType())

        input_df_lower = self.input_df.toDF(*[c.lower() for c in self.input_df.columns])
        if preview == 'T':
            if isinstance(column_name, list):
                input_df = input_df_lower.withColumn('concat_column', concat(*column_name))
                output_df = input_df.withColumn('concat_column_hash', hash_all_udf('concat_column'))
                output_df = output_df.select('concat_column', 'concat_column_hash')
                output_df = df_to_json(output_df.limit(limit_value))
            else:
                input_df = input_df_lower.withColumn(column_name + '_new', hash_all_udf(column_name))
                output_df = input_df.select(column_name, column_name + '_new')
                output_df = df_to_json(output_df.limit(limit_value))
            return output_df
        elif preview == 'F':
            if isinstance(column_name, list):
                input_df = input_df_lower.withColumn('concat_column', concat(*column_name))
                output_df = input_df.withColumn('concat_column', hash_all_udf('concat_column')).drop(*column_name)
            else:
                output_df = input_df_lower.withColumn(column_name, hash_all_udf(column_name))
            return output_df


class TwoWayEncryption:
    """
    | 양발향 암호화 구현한 클래스
    Args:
        input_df (spark.Dataframe) : 데이터프레임
        column_name (str): 컬럼
        key : secret key
        encoding (str) : 암호화 방식 'base64' or 'hex'
        preview (boolean) : 프리뷰 여부
    Sample:
        TwoWayEncryption(df).apply_encrypt(column_name='ptno',key='secret',encoding='base64',preview='T')

        - 암호화
        df_2 = TwoWayEncryption(df).apply_encrypt(column_name='ptno',key='secret',encoding='base64',preview='F')
        df_2.show(truncate=False)

        -복호화
        df_3 = TwoWayEncryption(df_2).apply_decrypt(column_name='ptno',key='secret',encoding='base64',preview='F')
        df_3.show(truncate=False)
    """

    def __init__(self, input_df):
        self.input_df = input_df

    def apply_encrypt(self, column_name, key, encoding, iv=None, preview='T'):
        def encrypt(raw, key, encoding, iv=None):
            if iv:
                iv = iv
            else:
                iv = b'\x00' * 16

            if not isinstance(raw, str):
                raw = str(raw)
            # key = self.key_gen(key)
            key = hashlib.sha256(key.encode()).digest()
            # raw = self._pad(raw.encode('utf-8'))
            s = raw.encode('utf-8')
            raw = s + (AES.block_size - len(s) % AES.block_size) * bytes([AES.block_size - len(s) % AES.block_size])
            cipher = AES.new(key, AES.MODE_CBC, iv)
            enc = cipher.encrypt(raw)
            if encoding == 'base64':
                return base64.b64encode(enc).decode('utf-8')
            elif encoding == 'hex':
                return enc.hex()
            else:
                raise ValueError('Unsupported encoding type: {}'.format(encoding))

        enc_udf = udf(lambda x: encrypt(raw=x, key=key, encoding=encoding), StringType())
        if preview == 'T':
            output = self.input_df.withColumn(column_name + "_new", enc_udf(self.input_df[column_name]))
            output_df = output.select(column_name, column_name + "_new")
            output_df = df_to_json(output_df.limit(limit_value))
            return output_df
        elif preview == 'F':
            output = self.input_df.withColumn(column_name + "_new", enc_udf(self.input_df[column_name]))
            output_df = output.drop(column_name).withColumnRenamed(column_name + "_new", column_name)
            output_df = output_df.select(*self.input_df.columns)
            return output_df

    def apply_decrypt(self, column_name, key, encoding, iv=None, preview='T'):
        def decrypt(raw, key, encoding, iv=None):
            if encoding == 'base64':
                enc = base64.b64decode(raw)
            elif encoding == 'hex':
                enc = bytes.fromhex(raw)
            else:
                raise ValueError('Unsupported encoding type: {}'.format(raw))

            if iv:
                iv = iv
            else:
                iv = b'\x00' * 16
            key = hashlib.sha256(key.encode()).digest()
            cipher = AES.new(key, AES.MODE_CBC, iv)
            dec = cipher.decrypt(enc)
            unpad = lambda s: s[:-ord(s[len(s) - 1:])]
            res = unpad(dec.decode('utf-8'))
            return res

        dec_udf = udf(lambda x: decrypt(raw=x, key=key, encoding=encoding), StringType())
        if preview == 'T':
            output = self.input_df.withColumn(column_name + "_new", dec_udf(self.input_df[column_name]))
            output_df = output.select(column_name, column_name + "_new")
            output_df = df_to_json(output_df.limit(limit_value))
            return output_df
        elif preview == 'F':
            output = self.input_df.withColumn(column_name + "_new", dec_udf(self.input_df[column_name]))
            output_df = output.drop(column_name).withColumnRenamed(column_name + "_new", column_name)
            output_df = output_df.select(*self.input_df.columns)
            return output_df



class FormatPreservingEncryptor:
    """
        | 형태보존 암호화 구현한 클래스
        Args:
            column_name (str): 컬럼
            data_type (str): 암호화 대상 컬럼의 데이터 타입 (uppercase_letters, lowercase_letters, letters, digits, alphanumeric, ascii)
            max_value_size (int): 암호화문의 최대길이
            key_size (int): 암/복호화에 사용할 key의 길이
            tweak (default=None):
            preview

        Sample:
            - 암호화
            df_2 = FormatPreservingEncryptor(df).apply_encrypt(column_name='age', data_type='letters', max_value_size=10, key_size=16,preview='F')
            df_2.show(truncate=False)

            -복호화
            df_3 = FormatPreservingEncryptor(df_2).apply_decrypt(column_name='age', data_type='letters', max_value_size=10, key_size=16,preview='T')
            df_3.show(truncate=False)

        """

    def __init__(self, input_data):
        self.input_data=input_data

    def apply_encrypt(self, column_name, data_type, max_value_size, key_size, tweak=None, preview='T'):
        def encrypt_fpe(raw, data_type, max_value_size, key_size, tweak=None):
            if isinstance(raw, int):  # 정수형인 경우 문자열로 변환
                raw = str(raw)

            row_bytes = raw.encode('utf-8')
            if data_type == 'uppercase_letters':
                max_value = 26 ** max_value_size
            elif data_type == 'lowercase_letters':
                max_value = 26 ** max_value_size
            elif data_type == 'letters':
                max_value = 52 ** max_value_size
            elif data_type == 'digits':
                max_value = 10 ** max_value_size
            elif data_type == 'alphanumeric':
                max_value = 62 ** max_value_size
            elif data_type == 'ascii':
                max_value = 128 ** max_value_size
            else:
                raise ValueError(f'Invalid data type: {data_type}')

            key = b'\x00' * key_size

            if tweak is None:
                tweak = b'\x00' * key_size

            cipher = Cipher(algorithms.AES(key), modes.CTR(tweak), backend=default_backend())

            row_to_int = int.from_bytes(row_bytes, 'big') % max_value
            encryptor = cipher.encryptor()

            ciphertext_int = int.from_bytes(
                encryptor.update(row_to_int.to_bytes(max_value.bit_length() + 7 // 8, 'big')), 'big')

            # ciphertext_hex = hex(ciphertext_int)[2:].zfill(max_value.bit_length() + 7 // 8 * 2)
            ciphertext_hex = '{:x}'.format(ciphertext_int).zfill(max_value.bit_length() + 7 // 8 * 2)

            return ciphertext_hex

        def encrypt_fpe_udf(data_type, max_value_size, key_size, tweak=None):
            def udf_wrapper(raw):
                return encrypt_fpe(raw, data_type, max_value_size, key_size, tweak)

            return udf_wrapper

        enc_udf = F.udf(encrypt_fpe_udf(data_type, max_value_size, key_size, tweak), StringType())
        output = self.input_data.withColumn(column_name+"_new", enc_udf(self.input_data[column_name]))
        if preview == 'T':
            output_df = output.select(column_name, column_name + "_new")
            output_df = df_to_json(output_df.limit(limit_value))
            return output_df
        elif preview == 'F':
            output_df = output.drop(column_name).withColumnRenamed(column_name + "_new", column_name)
            output_df = output_df.select(*self.input_data.columns)
            return output_df

    def apply_decrypt(self,  column_name, data_type, max_value_size, key_size, tweak=None, preview='T'):
        def decrypt_fpe(ciphertext, data_type, max_value_size, key_size, tweak=None):
            ciphertext_int = int(ciphertext, 16)

            if data_type == 'uppercase_letters':
                max_value = 26 ** max_value_size
            elif data_type == 'lowercase_letters':
                max_value = 26 ** max_value_size
            elif data_type == 'letters':
                max_value = 52 ** max_value_size
            elif data_type == 'digits':
                max_value = 10 ** max_value_size
            elif data_type == 'alphanumeric':
                max_value = 62 ** max_value_size
            elif data_type == 'ascii':
                max_value = 128 ** max_value_size
            else:
                raise ValueError(f'Invalid data type: {data_type}')

            key = b'\x00'*(key_size)

            if tweak is None:
                tweak = b'\x00' * key_size
                cipher = Cipher(algorithms.AES(key), modes.CTR(tweak), backend=default_backend())
            else:
                cipher = Cipher(algorithms.AES(key), modes.CTR(tweak), backend=default_backend())

            decryptor = cipher.decryptor()
            plaintext_int = int.from_bytes(
                decryptor.update(ciphertext_int.to_bytes(max_value.bit_length(), 'big')), 'big')

            plaintext_bytes = plaintext_int.to_bytes(max_value.bit_length(), 'big')
            plaintext_bytes = plaintext_bytes.strip(b'\x00')
            plaintext = plaintext_bytes.decode('utf-8')
            return plaintext

        def decrypt_fpe_udf(data_type, max_value_size, key_size, tweak=None):
            def udf_wrapper(raw):
                return decrypt_fpe(raw, data_type, max_value_size, key_size, tweak)
            return udf_wrapper

        dec_udf = F.udf(decrypt_fpe_udf(data_type, max_value_size, key_size, tweak), StringType())
        output = self.input_data.withColumn(column_name + "_new", dec_udf(self.input_data[column_name]))

        if preview == 'T':
            output_df = output.select(column_name, column_name + "_new")
            output_df = df_to_json(output_df.limit(limit_value))
            return output_df
        elif preview == 'F':
            output_df = output.drop(column_name).withColumnRenamed(column_name + "_new", column_name)
            output_df = output_df.select(*self.input_data.columns)
            return output_df
