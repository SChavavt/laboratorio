class DropdownChips {
    init(params) { this.gui = document.createElement('div'); this.refresh(params); }
    getGui() { return this.gui; }
    refresh(params) {
        const spec = params.context.dropdowns[params.colDef.field];
        const text = String(spec.aliases[String(params.value || '')] ?? params.value ?? '').trim();
        const exact = spec.values.find(value => value.trim() === text);
        const values = !text ? [] : !spec.multiple ? [exact || text] : SPLIT_DROPDOWN_VALUES(text, spec.values);
        this.gui.className = 'lab-dropdown-chips';
        this.gui.replaceChildren();
        values.forEach(value => {
            const native = spec.values.find(option => option.trim() === value.trim()) || value;
            const chip = document.createElement('span');
            chip.className = 'lab-dropdown-chip';
            chip.textContent = spec.labels[native] || native;
            chip.title = chip.textContent;
            const colors = spec.colors[native] || ['#EDE9FE', '#4C1D95'];
            chip.style.backgroundColor = colors[0];
            chip.style.color = colors[1];
            this.gui.appendChild(chip);
        });
        return true;
    }
}
