class AlignersStatusEditor {
    init(params) {
        this.params = params;
        this.value = params.value || "";
        const idColumn = params.context.idColumn || "No. Orden";
        const identifier = params.data[idColumn];
        const options = params.context.stageOptions[identifier] || [this.value];
        const palette = params.context.palettes.STATUS || {};

        this.gui = document.createElement("div");
        this.gui.className = "lab-status-editor ag-custom-component-popup";
        this.gui.setAttribute("role", "listbox");
        this.gui.setAttribute("aria-label", "Etapas permitidas para el pedido");

        const title = document.createElement("strong");
        title.textContent = "Etapa permitida";
        this.gui.appendChild(title);

        options.forEach((option, index) => {
            const colors = palette[option] || ["#F3F0F8", "#392365"];
            const button = document.createElement("button");
            button.type = "button";
            button.textContent = option;
            button.style.backgroundColor = colors[0];
            button.style.color = colors[1];
            button.style.borderColor = colors[1] + "55";
            button.setAttribute("role", "option");
            button.setAttribute("aria-selected", String(option === this.value));
            button.addEventListener("click", () => {
                this.value = option;
                params.stopEditing();
            });
            button.addEventListener("keydown", event => {
                if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                    event.preventDefault();
                    const siblings = Array.from(this.gui.querySelectorAll("button"));
                    const direction = event.key === "ArrowDown" ? 1 : -1;
                    siblings[(siblings.indexOf(button) + direction + siblings.length) % siblings.length].focus();
                }
                if (event.key === "Escape") {
                    this.value = params.value || "";
                    params.stopEditing();
                }
            });
            this.gui.appendChild(button);
            if (option === this.value) this.initialButton = button;
            if (!this.initialButton && index === 0) this.initialButton = button;
        });
    }

    getGui() { return this.gui; }
    afterGuiAttached() { this.initialButton?.focus({preventScroll: true}); }
    isPopup() { return true; }
    getValue() { return this.value; }
}
