class DropdownEditor {
    init(params) {
        this.params = params;
        this.spec = params.context.dropdowns[params.colDef.field];
        this.original = String(params.value || '');
        const valueText = String(this.spec.aliases[this.original] ?? this.original);
        const exact = this.spec.values.find(value => value.trim() === valueText.trim());
        const parts = !valueText.trim() ? [] : !this.spec.multiple
            ? [exact || valueText] : SPLIT_DROPDOWN_VALUES(valueText, this.spec.values);
        this.values = [...new Set([...this.spec.values, ...parts])];
        this.selected = new Set(parts);
        this.gui = document.createElement('div');
        this.gui.className = 'lab-dropdown-editor';
        const title = document.createElement('strong');
        title.textContent = this.spec.multiple ? 'Elige una o varias opciones' : 'Elige una opción';
        this.gui.appendChild(title);
        this.search = document.createElement('input');
        this.search.type = 'search';
        this.search.placeholder = 'Buscar opción…';
        this.search.setAttribute('aria-label', 'Buscar opción');
        this.gui.appendChild(this.search);
        this.list = document.createElement('div');
        this.list.className = 'lab-dropdown-list';
        this.gui.appendChild(this.list);
        this.search.addEventListener('input', () => this.renderOptions());
        this.gui.addEventListener('keydown', event => {
            if (event.key === 'Escape') { this.cancelled = true; params.stopEditing(); }
            if (event.key === 'Enter') { event.preventDefault(); params.stopEditing(); }
            event.stopPropagation();
        });
        const actions = document.createElement('div');
        actions.className = 'lab-apparatus-actions';
        [['Limpiar', () => { this.selected.clear(); this.renderOptions(); }],
         ['Aplicar', () => params.stopEditing()],
         ['Cancelar', () => { this.cancelled = true; params.stopEditing(); }]].forEach(([label, action]) => {
            const button = document.createElement('button'); button.type = 'button';
            button.textContent = label; button.addEventListener('click', action); actions.appendChild(button);
        });
        this.gui.appendChild(actions);
        this.renderOptions();
    }
    renderOptions() {
        this.list.replaceChildren();
        const query = this.search.value.trim().toLocaleLowerCase();
        this.values.filter(value => String(this.spec.labels[value] || value).toLocaleLowerCase().includes(query)).forEach(value => {
            const button = document.createElement('button'); button.type = 'button';
            button.className = 'lab-dropdown-option';
            button.setAttribute('aria-pressed', String(this.selected.has(value)));
            const colors = this.spec.colors[value] || ['#F1F5F9', '#475569'];
            button.style.backgroundColor = colors[0]; button.style.color = colors[1];
            button.textContent = (this.selected.has(value) ? '✓ ' : '') + (this.spec.labels[value] || value);
            button.addEventListener('click', () => {
                if (!this.spec.multiple) { this.selected = new Set([value]); this.params.stopEditing(); }
                else { this.selected.has(value) ? this.selected.delete(value) : this.selected.add(value); this.renderOptions(); }
            });
            this.list.appendChild(button);
        });
    }
    getGui() { return this.gui; }
    afterGuiAttached() { this.search.focus(); }
    getValue() { return this.cancelled ? this.original : [...this.selected].join(', '); }
    isCancelAfterEnd() { return !!this.cancelled; }
    isPopup() { return true; }
}
