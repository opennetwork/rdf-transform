import { transform } from "../esnext/index.js";
import { Dataset } from "@opennetwork/rdf-dataset"
import {DefaultDataFactory} from "@opennetwork/rdf-data-model";

async function *thing() {
  yield 1
  yield ""
  yield new Date()
  yield {
    a: 1
  }
  yield [1]
  yield 1n
  yield -1n
  yield -0n
  yield 0n
  yield function *() {
    yield "Hello"
  }
}

async function run() {

  const dataset = new Dataset()
  const graph = DefaultDataFactory.blankNode(".")
  const source = transform(thing, {
    literalQuad: {
      subject: {
        termType: "NamedNode",
        value: "https://example.com"
      },
      graph: graph
    },
    profileQuad: {
      graph: DefaultDataFactory.blankNode("profile")
    }
  })
  await dataset.import(source)

  console.log(JSON.stringify(dataset.toArray(), undefined, "  "))
}

run()
  .then(() => {
    process.exit(0)
  })
  .catch(error => {
    console.error(error)
    process.exit(1)
  })
