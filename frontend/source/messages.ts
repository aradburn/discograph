/**
 * Shows a message in the message container
 */
export const showMessage = (message: string, type: string = "info"): void => {
    const container = document.querySelector("#messages");
    if (!container) return;

    const messageElement = document.createElement("div");
    messageElement.className = `alert alert-${type} alert-dismissible fade show`;
    messageElement.setAttribute("role", "alert");
    messageElement.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    container.appendChild(messageElement);
};

/**
 * Clears all messages from the message container
 * @param delay - Optional delay in milliseconds before clearing messages
 */
export const clearMessages = (delay: number = 0): void => {
    const clear = () => {
        const container = document.querySelector("#messages");
        if (container) {
            container.innerHTML = "";
        }
    };

    if (delay > 0) {
        window.setTimeout(clear, delay);
    } else {
        clear();
    }
};
