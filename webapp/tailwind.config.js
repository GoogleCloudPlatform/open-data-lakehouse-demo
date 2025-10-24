/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
        './buses-dashboard/templates/**/*.html', // Scans all .html files in the templates folder
        './buses-dashboard/static/webapp/**/*.js',    // Include if you have JS files with Tailwind classes
        './buses-dashboard/static/webapp/**/*.css'    // Include if you have JS files with Tailwind classes
    ],
    theme: {
        extend: {},
    },
    plugins: [],
}