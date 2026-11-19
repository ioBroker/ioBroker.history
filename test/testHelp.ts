import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import { COMMANDS, HELP } from '../src/lib/help';

/** The commands `processMessage` in src/main.ts answers */
function commandsOfMain(): string[] {
    const source = fs.readFileSync(path.join(__dirname, '../src/main.ts'), 'utf8');
    const start = source.indexOf('processMessage(msg');
    const end = source.indexOf('processStartValues()', start);
    assert.ok(start > 0 && end > start, 'processMessage found');
    return [...source.slice(start, end).matchAll(/msg\.command === '([^']+)'/g)].map(m => m[1]);
}

/** Whether the text names the command as a word of its own */
function names(text: string, command: string): boolean {
    return new RegExp(`(^|[^\\w])${command}([^\\w]|$)`).test(text);
}

describe('Test help', function () {
    it('lists every command processMessage answers, each once', function () {
        const commands = COMMANDS.map(c => c.command);
        assert.strictEqual(new Set(commands).size, commands.length, 'no duplicates');
        assert.deepStrictEqual([...commands].sort(), [...new Set(commandsOfMain())].sort());
    });

    it('describes every command', function () {
        const list = HELP.slice(HELP.indexOf('Commands (sendTo history.0):'));
        for (const { command } of COMMANDS.filter(c => c.command !== 'help')) {
            assert.ok(list.includes(`- ${command} `) || list.includes(`- ${command}:`), command);
        }
    });

    it('reserves exactly the commands with write: true for the request of the user', function () {
        const rules = HELP.slice(HELP.indexOf('How to talk to the user:'));
        const rule = rules.split('\n- ').find(r => r.includes("user's explicit"));
        assert.ok(rule, 'rule for the commands that change something');
        for (const { command, write } of COMMANDS) {
            assert.strictEqual(names(rule, command), write, command);
        }
    });

    it('keeps the lines short', function () {
        for (const line of HELP.split('\n')) {
            assert.ok(line.length <= 120, line);
        }
    });
});
