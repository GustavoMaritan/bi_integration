const fs = require('fs'),
    path = require('path');
let versionValue = null,
    versionUpdate = null,
    commitMessage = '';
const { exec } = require('child_process');

(async () => {
    versionUpdate = process.argv[2];
    commitMessage = process.argv[3];

    if (!versionUpdate) return console.log('Versão não informada.');
    if (!['patch', 'minor', 'major'].includes(versionUpdate)) return console.log("Versão informada não existe, tipos:['patch', 'minor', 'major']");
    versionValue = versionUpdate == 'patch' ? 2 : versionUpdate == 'minor' ? 1 : 0;

    await update();
})();

async function update() {
    try {
        await prom('git pull');

        const pack = fs.readFileSync(path.join(process.cwd(), 'package.json')),
            obj = JSON.parse(pack);

        let read = fs.readFileSync(path.join(process.cwd(), 'readme.md'), 'UTF-8');

        let a = obj.version.split('.'),
            oldVersion = obj.version;

        a[versionValue] = +a[versionValue] + 1;

        if (versionUpdate == 'major')
            a[1] = 0; a[2] = 0;

        obj.version = a.join('.');

        fs.writeFileSync(path.join(process.cwd(), 'package.json'), JSON.stringify(obj, null, 4));

        if (read) {
            read = read.replace('Version v' + oldVersion, 'Version v' + obj.version);
            fs.writeFileSync(path.join(process.cwd(), 'readme.md'), read);
        }
        await prom('git add .');
        await prom(`git commit -m "${commitMessage || (`Update -${versionUpdate}- para versão v${obj.version}`)}"`);
        await prom('git push');
        await prom(`git tag v${obj.version}`);
        await prom(`git push origin v${obj.version}`);
        await prom(`git tag`);

        end();
    } catch (ex) {
        end(ex);
    }
}

async function prom(command) {
    return new Promise((resolve, reject) => {
        exec(command, (err, out) => {
            if (err) return reject(err);
            if (out && out.trim()) console.log(out);
            resolve(out);
        })
    });
}

function end(ex) {
    if (ex) {
        console.log('### ERROUU ###');
        console.log(ex);
    } else
        console.log(':: EXIT....');
    console.log('______________________________________________________________________');
}