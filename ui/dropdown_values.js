function(value, options) {
    const text = String(value || '').trim();
    if (!text) return [];
    const native = options.find(option => option.trim() === text);
    if (native) return [native];
    const candidates = [...options].sort((a, b) => b.trim().length - a.trim().length);
    const parse = rest => {
        if (!rest) return [];
        for (const option of candidates) {
            const token = option.trim();
            if (!token) continue;
            if (rest === token) return [option];
            if (rest.startsWith(token)) {
                const tail = rest.slice(token.length);
                const separator = tail.match(/^\s*(?:,|\n)\s*/);
                if (separator) {
                    const following = parse(tail.slice(separator[0].length));
                    if (following !== null) return [option, ...following];
                }
            }
        }
        return null;
    };
    return [...new Set(parse(text) || text.split(/,\s*|\n/).map(part =>
        options.find(option => option.trim() === part.trim()) || part.trim()).filter(Boolean))];
}
