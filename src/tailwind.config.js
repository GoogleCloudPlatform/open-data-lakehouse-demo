/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [
        './open_data_lakehouse_demo/templates/**/*.html', // Scans all .html files in the templates folder
        './open_data_lakehouse_demo/static/src/**/*.js',    // Include if you have JS files with Tailwind classes
        './open_data_lakehouse_demo/static/src/**/*.css'    // Include if you have JS files with Tailwind classes
    ],
    theme: {
        extend: {},
    },
    plugins: [],
}