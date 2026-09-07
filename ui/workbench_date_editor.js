class WorkbenchDateEditor {
    init(params) {
        this.params = params;
        this.original = params.value || "";
        this.cancelled = false;
        this.changed = false;
        this.cleared = false;
        this.withTime = params.withTime;
        const originalIso = params.initialValues[this.original] ||
            (/^\d{4}\/\d{2}\/\d{2}( \d{2}:\d{2})?$/.test(this.original)
                ? this.original.replaceAll("/", "-").replace(" ", "T") : "");
        this.gui = document.createElement("div");
        this.gui.className = "lab-date-editor ag-custom-component-popup";
        this.gui.setAttribute("role", "group");
        this.gui.setAttribute("aria-label", params.colDef.headerName);
        const title = document.createElement("strong");
        title.textContent = params.colDef.headerName;
        this.gui.appendChild(title);
        const addInput = (type, label, value) => {
            const wrapper = document.createElement("label");
            wrapper.textContent = label;
            const input = document.createElement("input");
            input.type = type;
            input.value = value;
            input.setAttribute("aria-label", label);
            if (type === "time") input.step = "60";
            input.addEventListener("input", () => { this.changed = true; });
            wrapper.appendChild(input);
            this.gui.appendChild(wrapper);
            return input;
        };
        this.date = addInput("date", "Fecha", originalIso.slice(0, 10));
        if (this.withTime) this.time = addInput("time", "Hora", originalIso.slice(11, 16));
        const note = document.createElement("small");
        note.textContent = this.original && !originalIso
            ? "Valor anterior: " + this.original + ". Se conserva hasta que elijas una fecha."
            : "Hora de Ciudad de México · Después pulsa Guardar cambios.";
        this.gui.appendChild(note);
        const actions = document.createElement("div");
        actions.className = "lab-date-actions";
        const addButton = (label, action, primary = false) => {
            const button = document.createElement("button");
            button.type = "button";
            button.textContent = label;
            if (primary) button.className = "lab-date-primary";
            button.addEventListener("click", action);
            actions.appendChild(button);
        };
        addButton("Ahora", () => {
            const parts = new Intl.DateTimeFormat("en-CA", {
                timeZone: params.timeZone, year: "numeric", month: "2-digit", day: "2-digit",
                hour: "2-digit", minute: "2-digit", hourCycle: "h23"
            }).formatToParts(new Date());
            const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
            this.date.value = `${values.year}-${values.month}-${values.day}`;
            if (this.withTime) this.time.value = `${values.hour}:${values.minute}`;
            this.changed = true;
            params.stopEditing();
        });
        addButton("Borrar", () => {
            this.cleared = true;
            params.stopEditing();
        });
        addButton("Cancelar", () => {
            this.cancelled = true;
            params.stopEditing();
        });
        addButton("Aplicar", () => {
            if (!this.changed) return params.stopEditing();
            this.date.required = true;
            if (!this.date.reportValidity()) return;
            if (this.withTime) {
                this.time.required = true;
                if (!this.time.reportValidity()) return;
            }
            params.stopEditing();
        }, true);
        this.gui.appendChild(actions);
        this.gui.addEventListener("keydown", event => {
            if (event.key === "Escape") {
                this.cancelled = true;
                params.stopEditing();
            }
            /* Let the calendar/time controls handle their own arrows and Tab. */
            if (event.key !== "Escape") event.stopPropagation();
        });
    }
    getGui() { return this.gui; }
    afterGuiAttached() { this.date.focus({preventScroll: true}); }
    isPopup() { return true; }
    isCancelAfterEnd() {
        return this.cancelled || (this.changed && !this.cleared &&
            (!this.date.value || !this.date.checkValidity() ||
             (this.withTime && (!this.time.value || !this.time.checkValidity()))));
    }
    getValue() {
        if (this.cancelled) return this.original;
        if (this.cleared) return "";
        if (!this.changed || this.isCancelAfterEnd()) return this.original;
        /* ISO year first with explicit year; no browser-local timezone conversion. */
        return this.date.value.replaceAll("-", "/") + (this.withTime ? " " + this.time.value : "");
    }
}
