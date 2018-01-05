const Blowfish = require('blowfish');

class Crypt {

    constructor(key) {
        this.key = key;
    }

    encrypt(valor) {
        return this.blowfish.encrypt(valor);
    }

    decrypt(value) {
        return this.blowfish.decrypt(value).replace(/\0/g, '');
    }

    get blowfish() {
        if (!this.key)
            throw global.exception(400, 'Chave de criptografia não informada.', null);

        this.bf = this.bf || new Blowfish(this.key);
        return this.bf;
    }
}

module.exports = (key) => {
    return new Crypt(key);
};
