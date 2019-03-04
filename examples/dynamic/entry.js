const {helloWorld} = require("hello-world-npm");

module.exports = {
    test(key) {
        let sum = "";
        console.log(helloWorld());
        return [
            async ({ data }) => {
                console.log("line", key, data);
                sum += data + "\n";
            },
            () => {
                console.log(sum);
            },
        ];
    }
}