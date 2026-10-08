class AlignersStatusEditor {
    init(params) {
        this.params = params;
        this.value = params.value || "";
        const idColumn = params.context.idColumn || "No. Orden";
        const identifier = params.data[idColumn];
        this.options = params.context.stageOptions[identifier] || [this.value];
        this.manualOptions = (params.context.manualStageOptions || {})[identifier] || [];
        this.palette = params.context.palettes.STATUS || {};

        this.gui = document.createElement("div");
        this.gui.className = "lab-status-editor ag-custom-component-popup";
        this.gui.setAttribute("role", "dialog");
        this.gui.setAttribute("aria-label", "Elegir etapa del pedido");
        this.gui.addEventListener("keydown", event => {
            if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                event.preventDefault();
                const direction = event.key === "ArrowDown" ? 1 : -1;
                const index = this.buttons.indexOf(event.target);
                this.buttons[(index + direction + this.buttons.length) % this.buttons.length]?.focus();
            }
            if (event.key === "ArrowLeft" && this.manual) {
                event.preventDefault();
                this.render(false);
                this.initialButton?.focus({preventScroll: true});
            }
            if (event.key === "Escape") {
                event.preventDefault();
                this.value = params.value || "";
                params.stopEditing(true);
            }
        });
        this.render(false);
    }

    render(manual) {
        this.manual = manual;
        this.gui.replaceChildren();
        this.buttons = [];
        this.initialButton = null;
        const title = document.createElement("strong");
        title.textContent = manual ? "Todas las etapas del producto" : "Etapa permitida";
        this.gui.appendChild(title);
        if (manual) {
            this.addNavigation("← Volver a etapas sugeridas", false);
        }
        const list = document.createElement("div");
        list.className = "lab-status-options";
        list.setAttribute("role", "listbox");
        list.setAttribute("aria-label", title.textContent);
        this.gui.appendChild(list);
        const options = manual ? this.manualOptions : this.options;
        options.forEach((option, index) => {
            const colors = this.palette[option] || ["#F3F0F8", "#392365"];
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
                // Sólo una elección real deja una intención manual junto al
                // evento de la celda; navegar entre listas no modifica el pedido.
                this.params.data.__manualStageTarget = manual && !this.options.includes(option) ? option : null;
                this.params.stopEditing();
            });
            list.appendChild(button);
            this.buttons.push(button);
            if (option === this.value) this.initialButton = button;
            if (!this.initialButton && index === 0) this.initialButton = button;
        });
        if (!manual && this.manualOptions.some(option => !this.options.includes(option))) {
            this.addNavigation("Ver todas las etapas… →", true);
        }
    }

    addNavigation(label, manual) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "lab-status-navigation";
        button.textContent = label;
        if (manual) button.setAttribute("aria-haspopup", "listbox");
        button.addEventListener("click", () => {
            this.render(manual);
            this.initialButton?.focus({preventScroll: true});
        });
        this.gui.appendChild(button);
        this.buttons.push(button);
    }

    getGui() { return this.gui; }
    afterGuiAttached() { this.initialButton?.focus({preventScroll: true}); }
    isPopup() { return true; }
    getValue() { return this.value; }
}
