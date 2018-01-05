const adal = require('adal-node'),
    request = require('request-promise');

class Bi {

    /**
     * @constructor
     * @param {Object} config
     * @example
     * const Bi = new Bi({
     *     idAplicacao: "Id Aplicação",
     *     uriRecurso: "https://analysis.windows.net/powerbi/api",
     *     uriAutorizacao: "https://login.microsoftonline.com/{idAlgoQAueNaoSei}/oauth2/authorize",
     *     idGrupo: "",
     *     usuario: "",
     *     senha: "",
     *     key: "Chave para criptografia blowfish"
     * })
     */

    constructor(config) {
        this.idAplicacao = config.idAplicacao;
        this.uriRecurso = config.uriRecurso;
        this.uriAutorizacao = config.uriAutorizacao;
        this.idGrupo = config.idGrupo;
        this.usuario = config.usuario;
        this.senha = config.senha;
        this.key = config.key;

        if (this.key)
            this.crypt = require('./crypt')(config.key);
    }

    /**
     * Retorna token de acesso
     * @returns {String}
     * @public
     */
    async token() {
        let bi = this;
        return new Promise((resolve, reject) => {
            const context = new adal.AuthenticationContext(bi.uriAutorizacao);
            context.acquireTokenWithUsernamePassword(
                bi.uriRecurso,
                bi._user,
                bi._pass,
                bi.idAplicacao, (err, tokenResponse) => {
                    if (err) return reject(err);
                    resolve(tokenResponse.accessToken);
                });
        });
    }

    /**
     * Criar novo dataset
     * @param {Object} json 
     * 
     * @example
     * 
     * Bi.createDataSet({
     *      name: "Nome Dataset",
     *      tables: [{
     *            name: "Nome Tabela",
     *            columns: [{
     *                name: "Nome Coluna",
     *                dataType: "Type"
     *            }]
     *       }]
     *});
     */
    async createDataSet(json) {
        return await this._request(
            `https://api.PowerBI.com/v1.0/myorg/groups/${this.idGrupo}/datasets`,
            'POST',
            json
        );
    }

    /**
     * Retorna array de datasets do grupo
     * @returns {Array}
     */
    async getDatasets() {
        const results = await this._request(
            `https://api.PowerBI.com/v1.0/myorg/groups/${this.idGrupo}/datasets`,
            'GET',
        );
        return results && results.value ? results.value : [];
    }

    /**
     * Retorna id do dataset pelo nome informado.
     * @param {String} name
     * @returns {Number} - Retorna id do dateset
     */
    async getDatasetId(name) {
        const results = await this.getDatasets();

        let data = {};
        results.value.map(x => { if (x.name == name) data = x; });
        return data.id;
    }

    /**
     * Insere novos registros na tabela informada
     * @param {String} datasetName 
     * @param {String} tableName 
     * @param {Array} rows 
     * @returns {*}
     * @public
     */
    async addRows(datasetName, tableName, rows) {
        const datasetId = await this.getDatasetId(datasetName),
            results = await this._request(
                `https://api.powerbi.com/v1.0/myorg/groups/${this.idGrupo}/datasets/${datasetId}/tables/${tableName}/rows`,
                'POST',
                { rows: rows }
            );
        return results;
    };

    /**
     * Exclui todos registros da tabela informada.
     * @param {String} datasetName 
     * @param {String} tableName 
     * @public
     */
    async deleteRows(datasetName, tableName) {
        const datasetId = await this.getDatasetId(datasetName),
            results = await this._request(
                `https://api.powerbi.com/v1.0/myorg/groups/${this.idGrupo}/datasets/${datasetId}/tables/${tableName}/rows`,
                'DELETE'
            );
        return results;
    }

    /**
     * 
     * @private
     */
    async _request(url, method, body) {
        const token = await this.token(),
            options = {
                url: url,
                method: method,
                headers: { 'Authorization': `Bearer ${token}` },
                body: body,
                json: true
            };
        return await request(options);
    }

    /**
     * @private
     */
    get _user() {
        if (this._user) return this._user;
        this._user = this.key ? this.crypt(this.usuario) : this.usuario;
        return this._user;
    }

    /**
     * @private
     */
    get _pass() {
        if (this._pass) return this._pass;
        this._pass = this.key ? this.crypt(this.senha) : this.senha;
        return this._pass;
    }
}