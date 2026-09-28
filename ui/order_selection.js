class OrderSelection {
    init(params) {
        this.params = params;
        this.input = document.createElement('input');
        this.input.type = 'checkbox';
        this.input.className = 'lab-order-check';
        this.input.setAttribute('aria-label', 'Abrir pedido ' + (params.data['NOMBRE DOCTOR'] || ''));
        this.input.checked = params.value === true;
        this.input.addEventListener('pointerdown', event => event.stopPropagation());
        this.input.addEventListener('click', event => event.stopPropagation());
        this.input.addEventListener('keydown', event => event.stopPropagation());
        this.input.addEventListener('change', () => {
            this.params.node.setDataValue('SELECCIONAR', this.input.checked);
        });
    }
    getGui() { return this.input; }
    refresh(params) {
        this.params = params;
        this.input.checked = params.value === true;
        return true;
    }
}
