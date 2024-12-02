document.addEventListener('DOMContentLoaded', function () {
    const form = document.getElementById('prediction-form');
    const airlinesSelect = document.getElementById('airlines');
    const selectedAirlinesContainer = document.getElementById('selected-airlines');
    const resultDiv = document.getElementById('result');
    const errorDiv = document.getElementById('error');

    const selectedAirlines = new Set();

    // Hide result when any input changes
    const inputElements = form.querySelectorAll('input, select');
    inputElements.forEach(input => {
        input.addEventListener('change', function () {
            if (resultDiv) resultDiv.style.display = 'none';
            if (errorDiv) errorDiv.style.display = 'none';
        });
    });

    airlinesSelect.addEventListener('change', function () {
        const selectedOptions = Array.from(this.selectedOptions);
        selectedOptions.forEach(option => {
            if (!selectedAirlines.has(option.value)) {
                selectedAirlines.add(option.value);
                addAirlineTag(option.value);
            }
        });
        this.selectedIndex = -1; 
    });

    function addAirlineTag(airline) {
        const tag = document.createElement('div');
        tag.className = 'airline-tag';
        tag.innerHTML = `
            ${airline}
            <button class="remove-airline" data-airline="${airline}">&times;</button>
        `;
        selectedAirlinesContainer.appendChild(tag);
    }

    selectedAirlinesContainer.addEventListener('click', function (e) {
        if (e.target.classList.contains('remove-airline')) {
            const airline = e.target.getAttribute('data-airline');
            selectedAirlines.delete(airline);
            e.target.parentElement.remove();
        }
    });
});