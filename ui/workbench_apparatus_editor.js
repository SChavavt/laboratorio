class WorkbenchApparatusEditor {
    init(params) {
        this.params = params;
        this.catalog = params.context.apparatusOptions || [];
        this.originalValue = params.value || "";
        const selectedNames = String(this.originalValue)
            .split(/\s*\+\s*/)
            .map(value => value.trim())
            .filter(Boolean);
        this.selected = new Set(
            this.catalog.filter(option => selectedNames.includes(option))
        );
        this.value = this.canonicalValue();

        this.gui = document.createElement("div");
        this.gui.className = "lab-apparatus-editor ag-custom-component-popup";
        this.gui.setAttribute("role", "dialog");
        this.gui.setAttribute("aria-label", "Seleccionar uno o varios aparatos");

        const title = document.createElement("strong");
        title.textContent = "Combinar aparatos";
        this.gui.appendChild(title);

        this.summary = document.createElement("div");
        this.summary.className = "lab-apparatus-summary";
        this.gui.appendChild(this.summary);

        this.options = [];
        this.catalog.forEach(option => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "lab-apparatus-option";
            button.dataset.option = option;

            const dot = document.createElement("span");
            dot.className = "lab-apparatus-dot";
            const colors = (params.context.apparatusColors || {})[option] || ["#8C62D2", "#FFFFFF"];
            dot.style.backgroundColor = colors[0];
            dot.style.boxShadow = `0 0 0 1px ${colors[1]}44`;

            const label = document.createElement("span");
            label.textContent = option;
            const check = document.createElement("span");
            check.className = "lab-apparatus-check";

            button.append(dot, label, check);
            button.addEventListener("click", event => {
                event.preventDefault();
                if (this.selected.has(option)) this.selected.delete(option);
                else this.selected.add(option);
                this.value = this.canonicalValue();
                this.refresh();
            });
            this.options.push(button);
            this.gui.appendChild(button);
        });

        const actions = document.createElement("div");
        actions.className = "lab-apparatus-actions";
        const cancel = document.createElement("button");
        cancel.type = "button";
        cancel.className = "lab-apparatus-cancel";
        cancel.textContent = "Cancelar";
        cancel.addEventListener("click", () => {
            this.value = this.originalValue;
            params.stopEditing(true);
        });
        this.apply = document.createElement("button");
        this.apply.type = "button";
        this.apply.className = "lab-apparatus-apply";
        this.apply.textContent = "Aplicar combinación";
        this.apply.addEventListener("click", () => {
            if (!this.selected.size) return;
            this.value = this.canonicalValue();
            params.stopEditing();
        });
        actions.append(cancel, this.apply);
        this.gui.appendChild(actions);
        this.refresh();
    }

    canonicalValue() {
        return this.catalog.filter(option => this.selected.has(option)).join(" + ");
    }

    refresh() {
        this.summary.textContent = this.value || "Selecciona al menos un aparato";
        this.apply.disabled = this.selected.size === 0;
        this.options.forEach(button => {
            const selected = this.selected.has(button.dataset.option);
            button.setAttribute("aria-pressed", String(selected));
            button.querySelector(".lab-apparatus-check").textContent = selected ? "✓" : "+";
        });
    }

    getGui() { return this.gui; }
    afterGuiAttached() { this.options[0]?.focus({preventScroll: true}); }
    isPopup() { return true; }
    getValue() { return this.value || this.originalValue; }
}
