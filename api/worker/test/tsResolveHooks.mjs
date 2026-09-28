// Resolution hook: let a test import a src module that imports its siblings WITHOUT a file
// extension (`from "./util"`).
//
// esbuild - which is what actually builds the worker - resolves those, and so does tsc under
// `moduleResolution: "Bundler"`. Node's ESM resolver does not, so `import("../src/series.ts")`
// fails on the first extensionless specifier it meets. The alternative was adding `.ts` to 48
// import statements across 12 source files, i.e. touching almost every file in the worker for
// a test; this keeps the change inside test/.
//
// It only ever ADDS a `.ts` candidate for a relative specifier that has no extension, and
// falls through to the default resolver otherwise, so it cannot silently redirect an import
// that would have resolved on its own.
export async function resolve(specifier, context, next) {
  const relative = specifier.startsWith("./") || specifier.startsWith("../");
  if (relative && !/\.[cm]?[jt]s$|\.json$/.test(specifier)) {
    try {
      return await next(`${specifier}.ts`, context);
    } catch {
      /* fall through to the real resolver, so its error is the one reported */
    }
  }
  return next(specifier, context);
}
