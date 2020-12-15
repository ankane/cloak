module Cloak
  module Utils
    KEY_NONCE = "\x00".b*16
    MEMBER_NONCE = "\x01".b*16
    HLL_ELEMENT_NONCE = "\x02".b*16

    private

    def create_encryptor(key)
      @encryptor = Miscreant::AEAD.new("AES-SIV", [key].pack("H*"))
    end

    def encrypt_value(value)
      if value.nil?
        value
      else
        nonce = Miscreant::AEAD.generate_nonce
        nonce + @encryptor.seal(to_binary(value), nonce: nonce)
      end
    end

    def decrypt_value(value)
      if value.nil? || value.empty?
        value
      else
        value = to_binary(value)
        nonce = value.slice(0, 16)
        value = value.slice(16..-1)
        raise Error, "Decryption failed" if nonce.bytesize != 16 || value.nil?
        value = @encryptor.open(value, nonce: nonce)
        value.force_encoding(Encoding::UTF_8)
        value
      end
    end

    alias_method :encrypt_element, :encrypt_value
    alias_method :decrypt_element, :decrypt_value

    def encrypt_key(key)
      @encryptor.seal(to_binary(key), nonce: KEY_NONCE)
    end

    def decrypt_key(key)
      @encryptor.open(to_binary(key), nonce: KEY_NONCE)
    end

    def encrypt_field(key, field)
      @encryptor.seal(to_binary(field), nonce: key.slice(0, 16))
    end

    def decrypt_field(key, field)
      @encryptor.open(to_binary(field), nonce: key.slice(0, 16))
    end

    def encrypt_member(value)
      @encryptor.seal(to_binary(value), nonce: MEMBER_NONCE)
    end

    def decrypt_member(value)
      @encryptor.open(to_binary(value), nonce: MEMBER_NONCE)
    end

    def encrypt_hll_element(value)
      @encryptor.seal(to_binary(value), nonce: HLL_ELEMENT_NONCE)
    end

    def decrypt_hll_element(value)
      @encryptor.open(to_binary(value), nonce: HLL_ELEMENT_NONCE)
    end

    def to_binary(value)
      value = value.to_s
      value = value.dup.force_encoding(Encoding::BINARY) unless value.encoding == Encoding::BINARY
      value
    end
  end
end
