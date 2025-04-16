/**
 * Custom error types for specific application errors
 */
export class DiscographError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "DiscographError";
    }
}

export class NetworkDataError extends DiscographError {
    constructor(message: string) {
        super(message);
        this.name = "NetworkDataError";
    }
}

export class APIError extends DiscographError {
    constructor(message: string) {
        super(message);
        this.name = "APIError";
    }
}
