import { readFileSync } from "node:fs"
import ts from "typescript"

// Execute real classes with browser/native boundaries supplied by each test.
export function loadClasses(file, names, dependencies = {}) {
    const source = readFileSync(new URL(file, import.meta.url), "utf8")
        .replace(/^import .*$/gm, "")
        .replace(/^export /gm, "")
    const { outputText } = ts.transpileModule(source, {
        compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
    })
    return new Function(...Object.keys(dependencies), outputText + `\nreturn {${names.join(",")}}`)(...Object.values(dependencies))
}
