import {contains, is, transform} from "../esnext/index.js";
import { Dataset } from "@opennetwork/rdf-dataset"
import {DefaultDataFactory} from "@opennetwork/rdf-data-model";
import React from "react"
import ReactDOM from "react-dom/server.node.js"
import mime from "@opennetwork/rdf-namespace-mime"

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
  yield React.createElement("a", { href: "https://example.com", key: 1 })

  function Component() {
    return React.createElement("p", { }, "Hello")
  }

  yield React.createElement(Component)
}

const reactElement = DefaultDataFactory.namedNode("https://reactjs.org/element")

async function run() {

  const elements = new WeakMap()

  const dataset = new Dataset()
  const graph = DefaultDataFactory.blankNode(".")
  const source = transform(thing, {
    context: dataset,
    literalQuad: {
      subject: {
        termType: "NamedNode",
        value: "https://example.com"
      },
      graph: graph
    },
    profileQuad: {
      graph: DefaultDataFactory.blankNode("profile")
    },
    transformations: [
      async function *(source, options) {
        if (!React.isValidElement(source)) {
          return
        }
        const node = DefaultDataFactory.blankNode()
        yield DefaultDataFactory.quad(
          options.literalQuad.subject,
          reactElement,
          node,
          options.literalQuad.graph
        )
        // This can be later retrieved and rendered as part of this node
        // elements.set(node, source)

        const htmlString = await ReactDOM.renderToStaticMarkup(source)

        yield DefaultDataFactory.quad(
          node,
          mime.html,
          DefaultDataFactory.literal(
            htmlString,
            mime.html
          ),
          options.literalQuad.graph
        )

      }
    ]
  })
  await dataset.import(source, true)

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
