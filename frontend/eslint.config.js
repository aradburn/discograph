import eslint from "@eslint/js";
import tseslint from "@typescript-eslint/eslint-plugin";
import tsparser from "@typescript-eslint/parser";
import prettierPlugin from "eslint-plugin-prettier";
import prettierConfig from "eslint-config-prettier";
import globals from "globals";
import testingLibrary from "eslint-plugin-testing-library";

export default [
    // Base ESLint configuration
    {
        ignores: ["**/dist/**", "**/node_modules/**"],
        linterOptions: {
            reportUnusedDisableDirectives: true,
        },
    },

    // JavaScript files (during transition)
    {
        files: ["**/*.{js,jsx}"],
        languageOptions: {
            ecmaVersion: "latest",
            sourceType: "module",
            globals: {
                ...globals.browser,
                ...globals.es2021,
            },
        },
        plugins: {
            prettier: prettierPlugin,
        },
        rules: {
            ...eslint.configs.recommended.rules,
            "prefer-const": "warn",
            "no-unused-vars": "warn",
            "prettier/prettier": "warn",
        },
    },

    // TypeScript files
    {
        files: ["**/*.{ts,tsx}"],
        languageOptions: {
            parser: tsparser,
            parserOptions: {
                ecmaVersion: "latest",
                sourceType: "module",
                project: "./tsconfig.json",
                tsconfigRootDir: ".",
            },
            globals: {
                ...globals.browser,
                ...globals.es2021,
            },
        },
        plugins: {
            "@typescript-eslint": tseslint,
            prettier: prettierPlugin,
        },
        rules: {
            ...eslint.configs.recommended.rules,
            ...tseslint.configs["recommended"].rules,
            ...tseslint.configs["recommended-requiring-type-checking"].rules,
            "@typescript-eslint/no-explicit-any": "warn",
            "@typescript-eslint/explicit-function-return-type": "off",
            "@typescript-eslint/no-unused-vars": [
                "warn",
                {
                    argsIgnorePattern: "^_",
                    varsIgnorePattern: "^_",
                },
            ],
            "@typescript-eslint/consistent-type-imports": [
                "warn",
                {
                    prefer: "type-imports",
                },
            ],
            "prefer-const": "warn",
            "no-unused-vars": "off",
            "prettier/prettier": "warn",
        },
    },

    // Test files specific configuration
    {
        files: ["**/*.test.{ts,tsx}", "**/tests/**/*.{ts,tsx}"],
        plugins: {
            "testing-library": testingLibrary,
        },
        rules: {
            "@typescript-eslint/no-explicit-any": "off",
            "@typescript-eslint/no-unused-vars": "off",
            "@typescript-eslint/unbound-method": "off",
            "testing-library/no-node-access": "off",
            "testing-library/no-container": "off",
        },
        languageOptions: {
            globals: {
                ...globals.browser,
                ...globals.es2021,
                ...globals.node,
                describe: "readonly",
                it: "readonly",
                expect: "readonly",
                vi: "readonly",
                beforeEach: "readonly",
                afterEach: "readonly",
                beforeAll: "readonly",
                afterAll: "readonly",
            },
        },
    },

    // Apply Prettier config last to ensure it takes precedence
    prettierConfig,
];
